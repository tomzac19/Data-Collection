import os
import pandas as pd

# Define methods to obtain the final database
def merge_csv_files(source_dir, destination_dir):
    # Create a dictionary to store dataframes for each folder
    dfs = {}

    # Create a dictionary to store NPC and Emissions from pareto_point.csv
    pareto_data = {}

    # Iterate over files in the source directory
    for root, _, files in os.walk(source_dir):
        for file in files:
            # Check if the file is a CSV and not one of the excluded files
            if file.endswith('.csv') and file not in ['unmet_demand.csv', 'unmet_demand_annual.csv', 'pareto_point.csv']:

                folder_name = os.path.basename(root)
                file_path = os.path.join(root, file)

                # Read the CSV into a DataFrame
                df = pd.read_csv(file_path)

                # Add columns for the folder name (Scenario) and file name
                df['Scenario'] = folder_name
                original_file_name = os.path.splitext(file)[0]
                df['OriginalFileName'] = original_file_name

                # Store the DataFrame in the dictionary
                dfs.setdefault(folder_name, []).append(df)

            # Check for pareto_point.csv and extract NPC and Emissions
            if file == 'pareto_point.csv':
                folder_name = os.path.basename(root)
                file_path = os.path.join(root, file)
                
                # Read pareto_point.csv
                pareto_df = pd.read_csv(file_path)
                
                # Extract NPC and Emissions values (assuming they're in row 1, col 2 for NPC and col 3 for emissions)
                npc_value = pareto_df.iloc[0, 1]  # NPC is in row 1, col 2 (0-based index)
                emissions_value = pareto_df.iloc[0, 2]  # Emissions is in row 1, col 3 (0-based index)
                
                # Store the values in the pareto_data dictionary for each scenario
                pareto_data[folder_name] = {
                    'Scenario': folder_name,
                    'NPC': npc_value,
                    'Emissions': emissions_value
                }

    # Merge dataframes for each folder
    merged_dfs = {}
    for folder_name, dfs_list in dfs.items():
        merged_dfs[folder_name] = pd.concat(dfs_list, ignore_index=True)

    # Identify available scenarios (folders)
    available_scenarios = [str(i) for i in range(1, 11) if str(i) in merged_dfs]

    # Concatenate dataframes for available scenarios into a single dataframe
    if available_scenarios:
        merged_df = pd.concat([merged_dfs[scenario] for scenario in available_scenarios], ignore_index=True)

        # Drop the 'Unnamed: 0' (i.e. the first column of each .csv file), 'Emission' (i.e. column specifying the kind of emission), and 'Region' columns if they exist
        merged_df.drop(columns=[col for col in ['Unnamed: 0', 
                                                'Emission' 
                                                ] if col in merged_df], inplace=True)

        # Copy 'Datetime' values to 'Year' where 'Year' is empty
        if 'Year' in merged_df and 'Datetime' in merged_df:
            merged_df['Year'].fillna(merged_df['Datetime'], inplace=True)
            merged_df.drop(columns=['Datetime'], inplace=True)

        # Remove columns with all NaN/empty values in the merged DataFrame
        merged_df.dropna(axis=1, how='all', inplace=True)

        # Write the merged dataframe to an Excel file with a separate sheet for each 'OriginalFileName'
        excel_file_path = os.path.join(destination_dir, 'merged_db.xlsx')
        with pd.ExcelWriter(excel_file_path) as writer:
            # Create a separate sheet for each unique value in the 'OriginalFileName' column
            for original_file_name, group_df in merged_df.groupby('OriginalFileName'):
                # Remove 'OriginalFileName' column
                group_df = group_df.drop(columns=['OriginalFileName'])
                # Remove columns with all NaN/empty values before writing
                group_df.dropna(axis=1, how='all', inplace=True)
                group_df.to_excel(writer, index=False, sheet_name=original_file_name[:31])  # Limit sheet name length to 31 characters

            # Add a new sheet for NPC and Emissions data (pareto_point)
            if pareto_data:
                pareto_df = pd.DataFrame(pareto_data.values())  # Convert pareto_data dict to DataFrame
                # Remove columns with all NaN/empty values before writing
                pareto_df.dropna(axis=1, how='all', inplace=True)
                pareto_df.to_excel(writer, index=False, sheet_name='pareto_point')  # Write the pareto_data to the pareto_point sheet

        print(f"Merged file with separate sheets saved to {excel_file_path}")
    else:
        print("No valid scenarios found. No file created.")

# Apply the method above:
source_directory = r'C:\Users\Tommaso\Documents\GitHub\Data-Collection\pareto_frontier_example'
destination_directory = r'C:\Users\Tommaso\Documents\GitHub\Data-Collection\pareto_frontier_example'

merge_csv_files(source_directory, destination_directory)