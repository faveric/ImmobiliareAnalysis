import requests
import json
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_geography_data(id_num, type_num):
    """
    Fetches geographical data for a specific ID and type.

    Args:
        id_num: The ID to check (0-9999)
        type_num: The type of geography (1: region, 2: province, 3: city)

    Returns:
        Dictionary with the data if successful, None if not found or error
    """
    base_url = "https://www.immobiliare.it/api-next/geography/geography-lists/"
    params = {
        "id": f"{id_num:04d}",
        "type": type_num,
        "__lang": "it"
    }

    try:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            return {
                "id": f"{id_num:04d}",
                "type": type_num,
                "data": data
            }
    except:
        pass
    return None


def process_batch(id_range, type_nums):
    """
    Process a batch of IDs and types.
    """
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for id_num in id_range:
            for type_num in type_nums:
                futures.append(
                    executor.submit(get_geography_data, id_num, type_num)
                )

        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
    return results


def main():
    type_nums = [1, 2, 3]  # Region, Province, City
    all_results = []
    batch_size = 100

    # Create progress bar for all batches
    total_batches = (10000 + batch_size - 1) // batch_size

    for batch_start in tqdm(range(10000, 20000, batch_size), desc="Processing IDs"):
        batch_end = min(batch_start + batch_size, 20000)
        id_range = range(batch_start, batch_end)

        # Process batch
        batch_results = process_batch(id_range, type_nums)
        all_results.extend(batch_results)

        # Save intermediate results
        if batch_results:
            with open("geography_data.json", "w", encoding="utf-8") as f:
                json.dump(all_results, f, indent=2, ensure_ascii=False)

        # Small delay to avoid overwhelming the server
        time.sleep(0.1)

    print(f"\nFound {len(all_results)} valid responses")
    print("Results saved to geography_data.json")


if __name__ == "__main__":
    main()