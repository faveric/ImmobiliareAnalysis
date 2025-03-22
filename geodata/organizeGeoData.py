import json
import pandas as pd


def extract_geo_data(file_path):
    """
    Extract geographic data from a JSON file and create a DataFrame.

    For each type 3 entity, extract:
    - id, label, lat, lng from the entity itself
    - id and label from its parent type 1 (province)
    - id and label from its parent type 0 (region)

    Args:
        file_path (str): Path to the JSON file

    Returns:
        pandas.DataFrame: DataFrame containing the extracted data
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
    except json.JSONDecodeError:
        print("Error: The file contains invalid JSON format.")
        return pd.DataFrame()
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return pd.DataFrame()

    results = []

    for item in data:
        # Check if the item is of type 3
        if item.get('type') == 3:
            entity_data = item.get('data', {})

            # Extract entity information
            entity_info = {
                'entity_id': entity_data.get('id'),
                'entity_label': entity_data.get('label'),
                'entity_lat': entity_data.get('center', {}).get('lat'),
                'entity_lng': entity_data.get('center', {}).get('lng')
            }

            # Extract parent information (province type 1 and region type 0)
            province_info = {
                'province_id': None,
                'province_label': None
            }

            region_info = {
                'region_id': None,
                'region_label': None
            }

            for parent in entity_data.get('parents', []):
                if parent.get('type') == 1:
                    province_info = {
                        'province_id': parent.get('id'),
                        'province_label': parent.get('label')
                    }
                elif parent.get('type') == 0:
                    region_info = {
                        'region_id': parent.get('id'),
                        'region_label': parent.get('label')
                    }

            # Combine all information
            result = {**entity_info, **province_info, **region_info}
            results.append(result)

    # Create DataFrame from results
    df = pd.DataFrame(results)
    return df


def main():
    file_path = 'geography_data.json'
    output_csv = 'geo_data.csv'

    # Extract data to DataFrame
    df = extract_geo_data(file_path)

    # Check if DataFrame is empty
    if df.empty:
        print("No type 3 entities found or file couldn't be processed.")
        return

    # Save DataFrame to CSV
    df.to_csv(output_csv, index=False)

    # Display information
    print(f"Extracted {len(df)} type 3 entities.")
    print(f"Data saved to {output_csv}")
    print("\nDataFrame preview:")
    print(df.head())


if __name__ == "__main__":
    main()