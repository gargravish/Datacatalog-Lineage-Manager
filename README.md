# Custom Lineage Manager for Google Cloud Data Catalog

This script provides a command-line interface to manage custom lineage processes in Google Cloud Data Catalog. It allows you to:

  * List all lineage processes
  * Create new lineage
  * Delete single lineage process
  * Delete all lineage processes
  * View process details

## Prerequisites

  * Python 3.7 or later
  * Google Cloud Project with Data Catalog API enabled
  * Required Python packages:
    ```bash
    pip install google-cloud-datacatalog-lineage
    ```

## Usage

1.  Clone the repository:

    ```bash
    git clone [https://github.com/your-username/custom-lineage-manager.git](https://github.com/your-username/custom-lineage-manager.git)
    ```

2.  Set up authentication:

      * Follow the [Google Cloud authentication guide](https://www.google.com/url?sa=E&source=gmail&q=https://cloud.google.com/docs/authentication/getting-started) to set up your environment.

3.  Run the script:

    ```bash
    python Custom_Lineage_Manager.py
    ```

4.  Follow the on-screen menu to manage your custom lineage processes.

## Functions

  * `clear_screen()`: Clears the terminal screen.
  * `display_menu()`: Displays the main menu options.
  * `get_project_id()`: Prompts the user for their Google Cloud project ID.
  * `get_region()`: Prompts the user for a region with validation.
  * `list_processes(project_id, region)`: Lists all available lineage processes in the specified region.
  * `get_user_input_for_lineage()`: Collects user input for creating a new lineage.
  * `create_lineage(project_id, region, inputs)`: Creates a new lineage process with the provided inputs.
  * `delete_process(process_name)`: Deletes a specific lineage process.
  * `view_process_details(process_name)`: Displays detailed information about a specific process.

## Contributing

Contributions are welcome\! Feel free to open issues or pull requests.
