import os

from dwh2looker.logger import Logger
from github import Auth, Github, InputGitAuthor
from github.GithubException import UnknownObjectException

CONSOLE_LOGGER = Logger().get_logger()


class GithubClient:
    def __init__(
        self,
        token: str,
        repo: str,
        user_email: str = None,
        github_app: bool = False,
        main_branch: str = None,
    ):
        self.token = token
        self.repo = repo
        self.user_email = user_email
        # Create a GitHub API client using the access token
        auth = Auth.Token(token)
        g = Github(auth=auth)
        self.repo = g.get_repo(repo)
        self.user = g.get_user()
        self.github_app = github_app
        self.branch = main_branch

    def _get_author(self) -> InputGitAuthor:
        if self.github_app:
            if not self.user_email:
                raise ValueError(
                    "user_email is required when using a GitHub App for authentication"
                )
            user_name = self.user_email.split("@")[0]
            return InputGitAuthor(user_name, self.user_email)
        else:
            # PAT or normal user auth
            email = self.user_email or self.user.login
            return InputGitAuthor(self.user.login, email)

    def _get_or_create_branch(self, target_branch: str, base_branch: str):
        branches = [b.name for b in self.repo.get_branches()]

        if target_branch in branches:
            CONSOLE_LOGGER.info(f"Branch {target_branch} already exists")
        else:
            base_ref = self.repo.get_branch(base_branch)
            self.repo.create_git_ref(
                f"refs/heads/{target_branch}", sha=base_ref.commit.sha
            )
            CONSOLE_LOGGER.info(
                f"New branch {target_branch} created in repository {self.repo.name}"
            )

    def checkout_branch(self, branch_name: str, base_branch: str = "main"):
        self._get_or_create_branch(branch_name, base_branch)
        self.branch = self.repo.get_branch(branch_name)
        CONSOLE_LOGGER.info(f"Switched to branch {branch_name}")

    def get_folder_content(
        self, folder_path: str, branch_name: str = None
    ) -> list[str]:
        branch = branch_name or self.branch if self.branch else "main"
        try:
            contents = self.repo.get_contents(folder_path, ref=branch)
            return [content.name for content in contents]
        except UnknownObjectException:
            CONSOLE_LOGGER.warn(f"Folder {folder_path} not found in branch {branch}")
            return []

    def read_local_files(self, input_dir: str) -> list[dict[str, str]]:
        files = []
        for file_name in os.listdir(input_dir):
            file_path = os.path.join(input_dir, file_name)
            with open(file_path, "r", encoding="utf-8") as f:
                file_content = f.read()
            files.append({"name": file_name, "content": file_content})
        return files

    def _commit_files(
        self,
        files: list[dict[str, str]],
        output_dir: str,
        target_branch: str,
        author: InputGitAuthor,
        file_creation_message: str = None,
        file_update_message: str = None,
    ) -> bool:
        has_changes = False
        try:
            contents = self.repo.get_contents(output_dir, ref=target_branch)
            existing_files = {
                content_file.name: content_file for content_file in contents
            }
        except UnknownObjectException:
            existing_files = {}

        for file in files:
            output_path = os.path.join(output_dir, file["name"])

            if file["name"] in existing_files:
                content_file = existing_files[file["name"]]
                # Compare file contents
                try:
                    remote_content = content_file.decoded_content.decode("utf-8")
                except UnicodeDecodeError:
                    CONSOLE_LOGGER.warning(
                        f"Could not decode remote file {file['name']} as utf-8."
                    )
                    # Fallback or simply continue to next file
                    continue

                if remote_content.strip() == file["content"].strip():
                    CONSOLE_LOGGER.info(
                        f"File {file['name']} already exists and it is up to date"
                    )
                    continue

                has_changes = True
                message = file_update_message or f"update {file['name']}"
                self.repo.update_file(
                    path=output_path,
                    message=message,
                    content=file["content"],
                    sha=content_file.sha,
                    branch=target_branch,
                    author=author,
                )
                CONSOLE_LOGGER.info(f"File {file['name']} has been updated")
            else:
                has_changes = True
                message = file_creation_message or f"create {file['name']}"
                self.repo.create_file(
                    path=output_path,
                    message=message,
                    content=file["content"],
                    branch=target_branch,
                    committer=author,
                    author=author,
                )
                CONSOLE_LOGGER.info(f"File {file['name']} has been created")
        return has_changes

    def update_files(
        self,
        input_dir: str,
        output_dir: str,
        target_branch: str,
        base_branch: str = "main",
        file_creation_message: str = None,
        file_update_message: str = None,
    ):
        author = self._get_author()
        self._get_or_create_branch(target_branch, base_branch)
        files = self.read_local_files(input_dir)

        has_changes = self._commit_files(
            files,
            output_dir,
            target_branch,
            author,
            file_creation_message,
            file_update_message,
        )

        if not has_changes:
            CONSOLE_LOGGER.info(
                "No changes detected. No commits have been made to the repository"
            )
            return

    def create_pull_request(
        self,
        base_branch: str,
        target_branch: str,
        pr_title: str,
        pr_body: str,
        draft: bool = False,
    ):
        # Create a pull request if it does not exist
        pulls = self.repo.get_pulls(state="open", sort="created", base=base_branch)
        if target_branch in [pull.head.ref for pull in pulls]:
            CONSOLE_LOGGER.info(
                f"Pull request already exists for branch {target_branch}"
            )
            return

        # Create a new pull request
        pull_request = self.repo.create_pull(
            title=pr_title,
            body=pr_body,
            base=base_branch,
            head=f"{target_branch}",
            draft=draft,
        )
        CONSOLE_LOGGER.info(f"Pull request created: {pull_request.html_url}")

    def delete_branch(self, branch_name: str):
        try:
            ref = self.repo.get_git_ref(f"heads/{branch_name}")
            ref.delete()
            CONSOLE_LOGGER.info(f"Branch {branch_name} deleted")
        except UnknownObjectException:
            CONSOLE_LOGGER.warn(f"{branch_name} does not exist")
