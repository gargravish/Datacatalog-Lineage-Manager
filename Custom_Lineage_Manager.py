"""
Copyright (c) 2024 Ravish Garg (ravishgarg@google.com)

"""

from datetime import datetime, timedelta
import logging
import os
from google.cloud import datacatalog_lineage_v1 as lineage_v1
from google.cloud.datacatalog_lineage_v1 import EventLink, LineageEvent, Process, Origin

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

client = lineage_v1.LineageClient()

OWNER = "ravishgarg@google.com"
VALID_REGIONS = [
    "asia-east1", "asia-east2", "asia-northeast1", "asia-northeast2", "asia-northeast3",
    "asia-south1", "asia-south2", "asia-southeast1", "asia-southeast2", "australia-southeast1",
    "europe-central2", "europe-north1", "europe-west1", "europe-west2", "europe-west3",
    "europe-west4", "europe-west6", "northamerica-northeast1", "northamerica-northeast2",
    "southamerica-east1", "southamerica-west1", "us-central1", "us-east1", "us-east4",
    "us-west1", "us-west2", "us-west3", "us-west4"
]

def clear_screen():
    """Clear the terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

def display_menu():
    """Display the main menu options."""
    print("\n=== Custom Lineage Manager ===")
    print("1. List All Lineage Processes")
    print("2. Create New Lineage")
    print("3. Delete Single Lineage Process")
    print("4. Delete All Lineage Processes")
    print("5. View Process Details")
    print("0. Exit")
    return input("\nSelect an option (0-5): ").strip()

def get_project_id():
    """Get project ID from user."""
    print("\nEnter Google Cloud Project ID")
    print("Example: my-project-123")
    return input("Project ID: ").strip()

def get_region():
    """Get region from user with validation."""
    while True:
        print("\nEnter Region for lineage creation")
        print("Example: us-central1, europe-west1, asia-east1")
        print("Press Enter to see all available regions")
        region = input("Region: ").strip()
        
        if not region:
            print("\nAvailable regions:")
            for i, r in enumerate(VALID_REGIONS, 1):
                print(f"{i:2d}. {r}")
            region_num = input("\nSelect region number: ").strip()
            try:
                region = VALID_REGIONS[int(region_num) - 1]
            except (ValueError, IndexError):
                print("Invalid selection. Please try again.")
                continue
        
        if region in VALID_REGIONS:
            return region
        print(f"Invalid region. Please select from available regions.")

def list_processes(project_id, region):
    """List all available lineage processes."""
    parent = f"projects/{project_id}/locations/{region}"
    try:
        processes = list(client.list_processes(parent=parent))
        if not processes:
            print(f"\nNo lineage processes found in region {region}.")
            return []
        
        print(f"\nAvailable Processes in region {region}:")
        print("-" * 80)
        for i, process in enumerate(processes, 1):
            print(f"{i}. Process Name: {process.name}")
            print(f"   Display Name: {process.display_name}")
            if process.attributes:
                print("   Attributes:")
                for key, value in process.attributes.items():
                    print(f"     {key}: {value}")
            print("-" * 80)
        return processes
    except Exception as e:
        logger.error(f"Error listing processes: {e}")
        return []

def get_user_input_for_lineage():
    """Get all required inputs for creating a new lineage."""
    print("\n=== New Lineage Creation ===\n")
    
    inputs = {}
    
    # Process Display Name
    print("\nEnter Process Display Name")
    print("Example: PubSub_to_BigQuery_Ingestion")
    inputs['process_display_name'] = input("Process Name: ").strip()
    
    # Origin Name
    print("\nEnter Origin Name")
    print("Example: Daily_Data_Ingestion_Pipeline")
    inputs['origin_name'] = input("Origin Name: ").strip()
    
    # Source FQDN
    print("\nEnter Source Fully Qualified Name")
    print("Examples:")
    print("- PubSub: projects/my-project/topics/my-topic")
    print("- GCS: gs://my-bucket/path/to/file.csv")
    print("- BigQuery: bigquery:project.dataset.table")
    inputs['source'] = input("Source FQDN: ").strip()
    
    # Target FQDN
    print("\nEnter Target Fully Qualified Name")
    print("Examples:")
    print("- BigQuery: bigquery:project.dataset.table")
    print("- GCS: gs://my-bucket/output/")
    inputs['target'] = input("Target FQDN: ").strip()
    
    # Process State
    print("\nEnter Process State")
    print("Valid states: STARTED, COMPLETED, FAILED, ABORTED")
    state = input("State (default: COMPLETED): ").strip().upper()
    inputs['state'] = state if state else "COMPLETED"
    
    # Run Display Name
    print("\nEnter Run Display Name")
    print("Example: RUN_2024_01_25_001")
    inputs['run_display_name'] = input("Run Name: ").strip()
    
    return inputs

def create_lineage(project_id, region, inputs):
    """Create a new lineage process with provided inputs."""
    try:
        # Set up timestamps
        start_time = datetime.now() - timedelta(hours=1)
        process_start_time = start_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z"
        process_end_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z"
        
        # Create Origin object
        origin = Origin()
        origin.source_type = lineage_v1.Origin.SourceType.CUSTOM
        origin.name = inputs['origin_name']
        
        # Create the process
        parent = f"projects/{project_id}/locations/{region}"
        process = Process()
        process.display_name = inputs['process_display_name']
        process.attributes = {
            "owner": OWNER,
            "framework": "Custom Data Pipeline",
            "service": "Custom Lineage"
        }
        process.origin = origin
        
        # Create process
        response = client.create_process(parent=parent, process=process)
        process_id = response.name
        logger.info(f"Created process: {process_id}")
        
        # Create run
        run = lineage_v1.Run()
        run.start_time = process_start_time
        run.end_time = process_end_time
        run.state = inputs['state']
        run.display_name = inputs['run_display_name']
        run.attributes = {
            "owner": OWNER,
            "purpose": "Custom Data Lineage"
        }
        
        run_response = client.create_run(parent=process_id, run=run)
        run_id = run_response.name
        logger.info(f"Created run: {run_id}")
        
        # Create lineage event
        source = lineage_v1.EntityReference()
        target = lineage_v1.EntityReference()
        source.fully_qualified_name = inputs['source']
        target.fully_qualified_name = inputs['target']
        
        links = [EventLink(source=source, target=target)]
        lineage_event = LineageEvent(
            links=links,
            start_time=process_start_time,
            end_time=process_end_time
        )
        
        event_response = client.create_lineage_event(
            parent=run_id,
            lineage_event=lineage_event
        )
        logger.info(f"Created lineage event: {event_response.name}")
        
        print("\nLineage created successfully!")
        return True
    except Exception as e:
        logger.error(f"Error creating lineage: {e}")
        print(f"\nError: {str(e)}")
        return False

def delete_process(process_name):
    """Delete a specific lineage process."""
    try:
        request = lineage_v1.DeleteProcessRequest(name=process_name)
        operation = client.delete_process(request=request)
        operation.result()
        print(f"\nSuccessfully deleted process: {process_name}")
        return True
    except Exception as e:
        logger.error(f"Error deleting process: {e}")
        print(f"\nError: {str(e)}")
        return False

def view_process_details(process_name):
    """View detailed information about a specific process."""
    try:
        request = lineage_v1.GetProcessRequest(name=process_name)
        process = client.get_process(request=request)
        
        print("\nProcess Details:")
        print("-" * 80)
        print(f"Name: {process.name}")
        print(f"Display Name: {process.display_name}")
        if process.attributes:
            print("Attributes:")
            for key, value in process.attributes.items():
                print(f"  {key}: {value}")
        if process.origin:
            print("Origin:")
            print(f"  Source Type: {process.origin.source_type}")
            print(f"  Name: {process.origin.name}")
        print("-" * 80)
        return True
    except Exception as e:
        logger.error(f"Error getting process details: {e}")
        print(f"\nError: {str(e)}")
        return False

def main():
    while True:
        clear_screen()
        choice = display_menu()
        
        if choice == "0":
            print("\nExiting Custom Lineage Manager...")
            break
            
        project_id = get_project_id()
        region = get_region()
        
        if choice == "1":
            list_processes(project_id, region)
            input("\nPress Enter to continue...")
            
        elif choice == "2":
            inputs = get_user_input_for_lineage()
            create_lineage(project_id, region, inputs)
            input("\nPress Enter to continue...")
            
        elif choice == "3":
            processes = list_processes(project_id, region)
            if processes:
                process_num = input("\nEnter the number of the process to delete: ").strip()
                try:
                    process = processes[int(process_num) - 1]
                    if input(f"\nConfirm deletion of {process.display_name}? (y/n): ").lower() == 'y':
                        delete_process(process.name)
                except (ValueError, IndexError):
                    print("\nInvalid process number.")
            input("\nPress Enter to continue...")
            
        elif choice == "4":
            processes = list_processes(project_id, region)
            if processes:
                if input("\nConfirm deletion of ALL processes? (y/n): ").lower() == 'y':
                    for process in processes:
                        delete_process(process.name)
            input("\nPress Enter to continue...")
            
        elif choice == "5":
            processes = list_processes(project_id, region)
            if processes:
                process_num = input("\nEnter the number of the process to view: ").strip()
                try:
                    process = processes[int(process_num) - 1]
                    view_process_details(process.name)
                except (ValueError, IndexError):
                    print("\nInvalid process number.")
            input("\nPress Enter to continue...")
            
        else:
            print("\nInvalid option. Please try again.")
            input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()