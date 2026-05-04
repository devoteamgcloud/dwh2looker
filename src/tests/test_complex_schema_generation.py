from unittest.mock import Mock, patch
from dwh2looker.lookml_generator.lookml_generator import LookMLGenerator
from dwh2looker.db_client.db_client import Field, Table


def test_schema_nested_field_generation():
    """
    Test generating LookML for a schema with a complex mix of:
    - primitive arrays (hobbies, recent_scores)
    - RECORD REPEATED (departments, order_history, project_involvement)
    - Deeply nested primitive arrays inside RECORD REPEATED (audit_log -> features_used, project_involvement -> tasks_completed)
    - Nested RECORD REPEATED inside RECORD REPEATED (departments -> employees)
    """

    # 1. Define fields based on the provided schema

    # --- TOP LEVEL FIELDS ---
    age_field = Field(name="age", internal_type="INT64", mode="NULLABLE", description="")
    
    current_address_field = Field(name="current_address", internal_type="RECORD", mode="NULLABLE", description="")
    city_field = Field(name="city", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="current_address", parent_field_type="RECORD_NULLABLE")
    street_address_field = Field(name="street_address", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="current_address", parent_field_type="RECORD_NULLABLE")
    zip_code_field = Field(name="zip_code", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="current_address", parent_field_type="RECORD_NULLABLE")
    
    state_info_field = Field(name="state_info", internal_type="RECORD", mode="NULLABLE", description="", parent_field_name="current_address", parent_field_type="RECORD_NULLABLE")
    code_field = Field(name="code", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="current_address.state_info", parent_field_type="RECORD_NULLABLE")
    country_code_field = Field(name="country_code", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="current_address.state_info", parent_field_type="RECORD_NULLABLE")
    name_field = Field(name="name", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="current_address.state_info", parent_field_type="RECORD_NULLABLE")

    email_field = Field(name="email", internal_type="STRING", mode="NULLABLE", description="")
    is_active_field = Field(name="is_active", internal_type="BOOL", mode="NULLABLE", description="")
    last_login_at_field = Field(name="last_login_at", internal_type="TIMESTAMP", mode="NULLABLE", description="")
    lifetime_value_field = Field(name="lifetime_value", internal_type="FLOAT64", mode="NULLABLE", description="")
    signup_date_field = Field(name="signup_date", internal_type="DATE", mode="NULLABLE", description="")
    user_id_field = Field(name="user_id", internal_type="INT64", mode="NULLABLE", description="")

    user_profile_field = Field(name="user_profile", internal_type="RECORD", mode="NULLABLE", description="")
    first_name_field = Field(name="first_name", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="user_profile", parent_field_type="RECORD_NULLABLE")
    last_name_field = Field(name="last_name", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="user_profile", parent_field_type="RECORD_NULLABLE")
    title_field = Field(name="title", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="user_profile", parent_field_type="RECORD_NULLABLE")

    # --- ARRAYS (Primitive) ---
    hobbies_field = Field(name="hobbies", internal_type="STRING", mode="REPEATED", description="")
    hobbies_element_field = Field(name="hobbies", internal_type="STRING", mode="NULLABLE", description="Elements of the array field: hobbies", parent_field_name="hobbies", parent_field_type="ARRAY")

    recent_scores_field = Field(name="recent_scores", internal_type="INT64", mode="REPEATED", description="")
    recent_scores_element_field = Field(name="recent_scores", internal_type="INT64", mode="NULLABLE", description="Elements of the array field: recent_scores", parent_field_name="recent_scores", parent_field_type="ARRAY")

    # --- RECORD REPEATED (Nested Arrays) ---
    order_history_field = Field(name="order_history", internal_type="RECORD", mode="REPEATED", description="")
    order_date_field = Field(name="order_date", internal_type="DATE", mode="NULLABLE", description="", parent_field_name="order_history", parent_field_type="RECORD_REPEATED")
    order_id_field = Field(name="order_id", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="order_history", parent_field_type="RECORD_REPEATED")
    status_field = Field(name="status", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="order_history", parent_field_type="RECORD_REPEATED")
    total_amount_field = Field(name="total_amount", internal_type="FLOAT64", mode="NULLABLE", description="", parent_field_name="order_history", parent_field_type="RECORD_REPEATED")

    # --- DEEPLY NESTED RECORDS & ARRAYS ---
    # departments
    departments_field = Field(name="departments", internal_type="RECORD", mode="REPEATED", description="")
    department_name_field = Field(name="department_name", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="departments", parent_field_type="RECORD_REPEATED")
    
    # departments -> employees (RECORD REPEATED inside RECORD REPEATED)
    employees_field = Field(name="employees", internal_type="RECORD", mode="REPEATED", description="", parent_field_name="departments", parent_field_type="RECORD_REPEATED")
    employee_id_field = Field(name="employee_id", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="departments.employees", parent_field_type="RECORD_REPEATED")
    role_field = Field(name="role", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="departments.employees", parent_field_type="RECORD_REPEATED")

    # audit_log
    audit_log_field = Field(name="audit_log", internal_type="RECORD", mode="REPEATED", description="")
    event_timestamp_field = Field(name="event_timestamp", internal_type="TIMESTAMP", mode="NULLABLE", description="", parent_field_name="audit_log", parent_field_type="RECORD_REPEATED")
    event_type_field = Field(name="event_type", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="audit_log", parent_field_type="RECORD_REPEATED")
    
    device_context_field = Field(name="device_context", internal_type="RECORD", mode="NULLABLE", description="", parent_field_name="audit_log", parent_field_type="RECORD_REPEATED")
    browser_field = Field(name="browser", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="audit_log.device_context", parent_field_type="RECORD_NULLABLE")
    ip_address_field = Field(name="ip_address", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="audit_log.device_context", parent_field_type="RECORD_NULLABLE")

    # audit_log -> features_used (primitive array inside RECORD REPEATED)
    features_used_field = Field(name="features_used", internal_type="STRING", mode="REPEATED", description="", parent_field_name="audit_log", parent_field_type="RECORD_REPEATED")
    features_used_element_field = Field(name="features_used", internal_type="STRING", mode="NULLABLE", description="Elements of the array field: features_used", parent_field_name="audit_log.features_used", parent_field_type="ARRAY")

    # project_involvement
    project_involvement_field = Field(name="project_involvement", internal_type="RECORD", mode="REPEATED", description="")
    project_name_field = Field(name="project_name", internal_type="STRING", mode="NULLABLE", description="", parent_field_name="project_involvement", parent_field_type="RECORD_REPEATED")

    # project_involvement -> tasks_completed (primitive array inside RECORD REPEATED)
    tasks_completed_field = Field(name="tasks_completed", internal_type="STRING", mode="REPEATED", description="", parent_field_name="project_involvement", parent_field_type="RECORD_REPEATED")
    tasks_completed_element_field = Field(name="tasks_completed", internal_type="STRING", mode="NULLABLE", description="Elements of the array field: tasks_completed", parent_field_name="project_involvement.tasks_completed", parent_field_type="ARRAY")

    # Build schema tree
    mock_table = Table(name="dim_demo_showcase", internal_schema=[])
    mock_table.schema = {
        age_field: {},
        current_address_field: {
            city_field: {},
            street_address_field: {},
            zip_code_field: {},
            state_info_field: {
                code_field: {},
                country_code_field: {},
                name_field: {}
            }
        },
        email_field: {},
        is_active_field: {},
        last_login_at_field: {},
        lifetime_value_field: {},
        signup_date_field: {},
        user_id_field: {},
        user_profile_field: {
            first_name_field: {},
            last_name_field: {},
            title_field: {}
        },
        hobbies_field: {
            hobbies_element_field: {}
        },
        recent_scores_field: {
            recent_scores_element_field: {}
        },
        order_history_field: {
            order_date_field: {},
            order_id_field: {},
            status_field: {},
            total_amount_field: {}
        },
        departments_field: {
            department_name_field: {},
            employees_field: {
                employee_id_field: {},
                role_field: {}
            }
        },
        audit_log_field: {
            event_timestamp_field: {},
            event_type_field: {},
            device_context_field: {
                browser_field: {},
                ip_address_field: {}
            },
            features_used_field: {
                features_used_element_field: {}
            }
        },
        project_involvement_field: {
            project_name_field: {},
            tasks_completed_field: {
                tasks_completed_element_field: {}
            }
        }
    }

    # 3. Setup LookMLGenerator
    with patch("dwh2looker.lookml_generator.lookml_generator.Config") as MockConfig:
        mock_config = Mock()
        MockConfig.return_value = mock_config
        mock_config.get_property.return_value = []

        def get_prop(prop, default=None):
            if prop == "primary_key_prefixes":
                return ["pk_"]
            if prop == "timeframes":
                return ["time", "date"]
            return default

        mock_config.get_property.side_effect = get_prop
        generator = LookMLGenerator(db_type="bigquery")

        # 4. Process views
        views = list(
            generator.process_views(
                schema=mock_table.schema,
                view_name=mock_table.name,
                sql_table_name="project.dataset.dim_demo_showcase",
            )
        )

        explore_content = generator.process_explore([v[1] for v in views][::-1]) # reverse needed per generation logic

        def get_view_content(name):
            for v_content, v_obj in views:
                if v_obj.name == name:
                    return v_content
            return None

        # Verify SQL for deep primitive arrays
        features_used_view = get_view_content("features_used")
        assert "sql: ${TABLE} ;;" in features_used_view

        tasks_completed_view = get_view_content("tasks_completed")
        assert "sql: ${TABLE} ;;" in tasks_completed_view

        hobbies_view = get_view_content("hobbies")
        assert "sql: ${TABLE} ;;" in hobbies_view

        recent_scores_view = get_view_content("recent_scores")
        assert "sql: ${TABLE} ;;" in recent_scores_view

        # Verify SQL for nested RECORD REPEATED elements
        employees_view = get_view_content("employees")
        assert "sql: ${TABLE}.employee_id ;;" in employees_view

        audit_log_view = get_view_content("audit_log")
        assert "sql: ${TABLE}.event_type ;;" in audit_log_view
        assert "sql: ${TABLE}.device_context.browser ;;" in audit_log_view

        order_history_view = get_view_content("order_history")
        assert "sql: ${TABLE}.status ;;" in order_history_view

        # Ensure correct explore generation structure (nested joins)
        assert "LEFT JOIN UNNEST(${dim_demo_showcase.departments}) AS dim_demo_showcase__departments" in explore_content
        assert "LEFT JOIN UNNEST(${dim_demo_showcase__departments.employees}) AS dim_demo_showcase__departments__employees" in explore_content
        assert "LEFT JOIN UNNEST(${dim_demo_showcase.audit_log}) AS dim_demo_showcase__audit_log" in explore_content
        assert "LEFT JOIN UNNEST(${dim_demo_showcase__audit_log.features_used}) AS dim_demo_showcase__audit_log__features_used" in explore_content
        assert "LEFT JOIN UNNEST(${dim_demo_showcase.project_involvement}) AS dim_demo_showcase__project_involvement" in explore_content
        assert "LEFT JOIN UNNEST(${dim_demo_showcase__project_involvement.tasks_completed}) AS dim_demo_showcase__project_involvement__tasks_completed" in explore_content

