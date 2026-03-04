import argparse

from dwh2looker.db_client import DbClient as db
from dwh2looker.diff_tracker import DiffTracker
from dwh2looker.logger import Logger
from dwh2looker.lookml_generator import LookMLGenerator

CONSOLE_LOGGER = Logger().get_logger()


def cli():
    parser = argparse.ArgumentParser(
        description="Command line interface for dwh2looker"
    )

    subparsers = parser.add_subparsers(dest="command", title="commands", metavar="")

    # track_diff_models parser
    diff_tracker_parser = subparsers.add_parser("diff_tracker", help="Run diff tracker")
    diff_tracker_parser.add_argument(
        "--db_type",
        type=str,
        help="Database type (bigquery, redshift, snowflake)",
        required=True,
    )
    diff_tracker_parser.add_argument(
        "--dataset1_name", type=str, help="Dataset 1", required=True
    )
    diff_tracker_parser.add_argument(
        "--dataset2_name", type=str, help="Dataset 2", required=True
    )
    diff_tracker_parser.add_argument(
        "--project", type=str, help="Project ID", required=True
    )
    # This argument is optional, but if it is not provided it will default to False
    diff_tracker_parser.add_argument(
        "--full-refresh",
        help="Full refresh of LookML base views",
        action=argparse.BooleanOptionalAction,
    )
    diff_tracker_parser.add_argument(
        "--service_account", type=str, help="Google Service Account", required=False
    )
    diff_tracker_parser.add_argument(
        "--models",
        type=str,
        help="List of models to compare (comma separated) or file path",
    )
    diff_tracker_parser.add_argument("--output", type=str, help="Output file path")

    # generate_lookml parser
    generate_lookml_parser = subparsers.add_parser(
        "generate_lookml", help="Run generate LookML"
    )
    generate_lookml_parser.add_argument(
        "--db_type",
        type=str,
        help="Database type (bigquery, redshift, snowflake)",
        required=True,
    )
    generate_lookml_parser.add_argument(
        "--override-dataset-id", type=str, help="Override Dataset ID", required=False
    )
    generate_lookml_parser.add_argument(
        "--service-account", type=str, help="Service Account", required=False
    )
    generate_lookml_parser.add_argument(
        "--token", type=str, help="GitHub Token", required=False
    )
    generate_lookml_parser.add_argument(
        "--github-app",
        action="store_true",
        help="Run as GitHub App (omit this flag if running with PAT)",
    )
    generate_lookml_parser.add_argument(
        "--push-lookml-to-looker",
        action="store_true",
        help="Push generated LookML to Looker via GitHub",
    )

    args = parser.parse_args()
    if args.command == "diff_tracker":
        models = args.models.split(",")
        if len(models) == 1:
            # If the tables argument is a file path, read the file and split on newlines
            if "." in models[0]:
                models_file_path = models[0]
                with open(models_file_path, "r") as file:
                    models = file.read().splitlines()

        CONSOLE_LOGGER.info(f"Models to be compared: {models}")

        credentials = {
            "service_account": args.service_account,
            "project_id": args.project,
            # Add other credentials for other databases here
        }

        db_client = db(db_type=args.db_type, credentials=credentials)

        dt = DiffTracker(
            dataset1_name=args.dataset1_name,
            dataset2_name=args.dataset2_name,
            db_client=db_client,
            models=models,
            full_refresh=args.full_refresh,
        )
        results = dt.get_diff_tables()
        CONSOLE_LOGGER.info(f"New models: {results['new_models']}")
        CONSOLE_LOGGER.info(f"Diff models: {results['diff_models']}")
        CONSOLE_LOGGER.info(f"Missing models: {results['missing_models']}")

        # Save output to file
        output = results["diff_models"]
        output += results["new_models"]
        with open(args.output, "w") as f:
            for o in output:
                f.write(f"{o}\n")
            pass

    elif args.command == "generate_lookml":
        lookml = LookMLGenerator(
            db_type=args.db_type,
            push_lookml_to_looker=args.push_lookml_to_looker,
            github_token=args.token,
            github_app=args.github_app,
        )
        lookml.generate_batch_lookml_views(
            override_dataset_id=args.override_dataset_id,
        )


def execute_from_command_line():
    cli()
