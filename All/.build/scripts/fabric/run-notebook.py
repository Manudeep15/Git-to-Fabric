import argparse
import sys
from shared import set_fabric_request_headers, get_fabric_workspace_by_name, get_fabric_notebook_by_name, run_notebook

def main():
    parser = argparse.ArgumentParser(description="Deploy to a Fabric deployment pipeline stage")
    parser.add_argument(
        "-t",
        "--tenant-id",
        dest="tenantId",
        type=str,
        required=True,
        help="The Azure Tenant ID the target Fabric Workspace belongs to.",
    )
    parser.add_argument(
        "-r",
        "--refresh-token",
        dest="refreshToken",
        type=str,
        required=True,
        help="The Refresh Token that is used to authenticate requests to the Fabric REST APIs. See the README for instructions on creating this if it does not exist.",
    )
    parser.add_argument(
        "-w",
        "--workspace-name",
        dest="workspaceName",
        type=str,
        required=True,
        help="The name of the Fabric Workspace that the notebook is being held in.",
    )
    parser.add_argument(
        "-nb",
        "--notebook-name",
        dest="notebookName",
        type=str,
        required=True,
        help="The Notebook to run.",
    )


    args = parser.parse_args()

    try:
        print("Setting Fabric request headers...", flush=True)
        base_url, fabric_headers = set_fabric_request_headers(args.tenantId, args.refreshToken)

        workspace = get_fabric_workspace_by_name(base_url, fabric_headers, args.workspaceName)
        notebook = get_fabric_notebook_by_name(base_url, fabric_headers, workspace, args.notebookName)

        if not workspace:
            raise Exception(f"A workspace with the name '{args.workspace_name}' was not found.")

        if workspace['displayName'] not in ['gs_tdm_dev', 'gs_tdm_sit']:
            if notebook['displayName'] not in ['nb_deploy_data_quality_checks_yaml_files']:
                # Only raise exception when notebooks other than whats specified in the above list are executed.
                raise Exception(f"Git update to workspace '{workspace['displayName']}' not allowed.")

        print("Getting Fabric Notebook...", flush=True)
        
        if notebook['displayName'] not in ['nb_test_runner', 'nb_system_integration_tests', 'nb_deploy_data_quality_checks_yaml_files']:
            raise Exception(f"Not allowed to execute notebook '{notebook['displayName']}'.")

        print(f"Running notebook {notebook['displayName']}...", flush=True)
        run_notebook(base_url, fabric_headers, workspace['id'], notebook['id'])

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
