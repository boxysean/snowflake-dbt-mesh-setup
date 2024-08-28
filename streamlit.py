import streamlit as st
import main
import functools
import traceback


if 'status_label' not in st.session_state:
    st.session_state['status_label'] = 'Not yet run'

if 'status_state' not in st.session_state:
    st.session_state['status_state'] = 'complete'


def deploy_wrapper(status):
    st.session_state.status_label = "Running..."
    st.session_state.status_state = "running"

    if not st.session_state.snowflake_account:
        st.session_state.status_label = "snowflake_account value required"
        st.session_state.status_state = "error"
        return

    if not st.session_state.snowflake_username:
        st.session_state.status_label = "snowflake_username value required"
        st.session_state.status_state = "error"
        return

    if not st.session_state.snowflake_password:
        st.session_state.status_label = "snowflake_password value required"
        st.session_state.status_state = "error"
        return

    if not st.session_state.dbt_cloud_account_id:
        st.session_state.status_label = "dbt_cloud_account_id value required"
        st.session_state.status_state = "error"
        return

    try:
        int(st.session_state.dbt_cloud_account_id)
    except ValueError:
        st.session_state.status_label = "dbt_cloud_account_id must be an integer"
        st.session_state.status_state = "error"
        return

    if not st.session_state.dbt_cloud_host:
        st.session_state.status_label = "dbt_cloud_host value required"
        st.session_state.status_state = "error"
        return

    status.update(label="Running...", state="running")

    try:
        main.deploy(
            snowflake_account=st.session_state.snowflake_account,
            snowflake_username=st.session_state.snowflake_username,
            snowflake_password=st.session_state.snowflake_password,
            dbt_cloud_service_token=st.session_state.dbt_cloud_service_token,
            dbt_cloud_account_id=st.session_state.dbt_cloud_account_id,
            dbt_cloud_host=st.session_state.dbt_cloud_host,
        )
    except main.SnowflakeError as e:
        st.session_state.status_label = "Snowflake failure!\n" + str(e)
        st.session_state.status_state = "error"
        print(traceback.format_exc())
    except main.DBTCloudError as e:
        st.session_state.status_label = "dbt Cloud failure!\n" + str(e)
        st.session_state.status_state = "error"
        print(traceback.format_exc())
    except Exception as e:
        st.session_state.status_label = "Failure!\n" + str(e)
        st.session_state.status_state = "error"
        print(traceback.format_exc())
    else:
        st.session_state.status_label = "Success!"
        st.session_state.status_state = "complete"


st.title("Build Data Products and a Data Mesh with dbt Cloud")

st.write("Use this app to quickly provision your Snowflake and dbt Cloud instances for the [Snowflake Quickstart guide](https://quickstarts.snowflake.com/guide/data-products-data-mesh-dbt-cloud/index.html#0).")

st.write("""Instructions:
- Fill out your configurations and credentials below.
- Please use a trial Snowflake Enterprise account and trial dbt Cloud Enterprise account.
- You must have a dbt Cloud Enterprise account in order to use this app.
- Use the tooltips on the inputs to learn where to find the information.
""")

with st.expander('Learn more about what this app does'):
    st.write('When you click "Provision", this app will run the following commands on Snowflake:')
    st.code("\n".join(main.FOUNDATIONAL_DB_SQL_STATEMENTS), language="sql")
    st.code("\n".join(main.FINANCE_DB_SQL_STATEMENTS), language="sql")
    st.code("\n".join(main.OTHER_SQL_STATEMENTS), language="sql")
    st.write("It will also run API calls on the [dbt Cloud Administrative API](https://docs.getdbt.com/docs/dbt-cloud-apis/admin-cloud-api) to setup your dbt Cloud and Snowflake to look like this:")
    st.image("image.png")
    st.write("No information is stored by this app.")

with st.form("my_form", clear_on_submit=False):
    st.subheader("Snowflake")

    snowflake_account = st.text_input(
        "Snowflake Account",
        key="snowflake_account",
        placeholder="abc123-def456",
        help="Your Snowflake account identifier, which can be found [here](https://docs.snowflake.com/en/user-guide/admin-account-identifier#finding-the-organization-and-account-name-for-an-account). For Snowflake trial accounts, this typically looks like 'abc123-def456'. Please note that the hyphen separator '-' is required rather than a dot '.'",
    )

    snowflake_username = st.text_input(
        "Snowflake Username",
        key="snowflake_username",
        placeholder="my_username",
        help="The Snowflake username you created with your new Snowflake trial account",
    )

    snowflake_password = st.text_input(
        "Snowflake Password",
        type='password',
        key="snowflake_password",
        placeholder="Mypassword123",
        help="The Snowflake password you created with your new Snowflake trial account",
    )

    st.subheader("dbt Cloud")

    dbt_cloud_service_token = st.text_input(
        "dbt Cloud Service Token with Account Admin privilege",
        type='password',
        key="dbt_cloud_service_token",
        placeholder="dbtc_XYZ123",
        help='[Follow these instructions](https://docs.getdbt.com/docs/dbt-cloud-apis/service-tokens#generate-service-account-tokens) to generate a dbt Cloud Service Token, and be sure to add Account Admin privileges to it.'
    )

    dbt_cloud_account_id = st.text_input(
        "dbt Cloud Account ID",
        key="dbt_cloud_account_id",
        placeholder="70000001234567",
        help="The account ID found in your dbt Cloud URL. Typically it starts with a 7 and is 14-digits long.",
    )

    dbt_cloud_host = st.text_input(
        "dbt Cloud Host",
        key="dbt_cloud_host",
        placeholder="ab123.us1.dbt.com",
        help="This is the host in the dbt Cloud URL. Common ones look like 'ab123.us1.dbt.com', or 'cloud.getdbt.com', or 'emea.dbt.com'.",
    )

    st.subheader("Provision")

    status = st.status(
        label=st.session_state.status_label,
        state=st.session_state.status_state,
    )

    st.form_submit_button('Provision', on_click=functools.partial(deploy_wrapper, status))

st.write("In you have any feedback or require any help with this app, please email: [sean.mcintyre@dbtlabs.com](mailto:sean.mcintyre@dbtlabs.com).")
