import argparse
import sys
from shared import set_fabric_request_headers, update_fabric_workspace_from_git


def main():
    parser = argparse.ArgumentParser(description="Update a Fabric workspace from Git.")
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
        help="The name of the Fabric Workspace that is being updated from its associated Git configuration.",
    )

    args = parser.parse_args()

    try:
        print("Setting Fabric request headers...", flush=True)
        base_url, fabric_headers = set_fabric_request_headers(
            args.tenantId, args.refreshToken
        )

        print(f"Updating {args.workspaceName} with main branch...", flush=True)
        update_fabric_workspace_from_git(base_url, fabric_headers, args.workspaceName)

    except Exception as e:
        print(f"Error: {str(e)}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
