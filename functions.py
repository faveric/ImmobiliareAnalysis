import streamlit as st
import requests
import json
import pandas as pd
import numpy as np
import time
import re
import plotly.express as px
from concurrent.futures import ThreadPoolExecutor, as_completed
import plotly.colors

def read_page(url, session="", retries=3, delay=2):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    for attempt in range(retries):
        try:
            if session:
                response = session.get(url, headers=headers)
            else:
                response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = json.loads(response.text)
                results = data.get('results', [])
                total_count = data.get('count', 0)
                max_pages = data.get('maxPages', 0)
                current_page = data.get('currentPage', 1)

                if not results:
                    return pd.DataFrame(), 0, True, 0, 0

                # Flatten nested JSON using pandas json_normalize
                df = pd.json_normalize(
                    results,
                    record_path=['realEstate', 'properties'],
                    meta=[
                        ['realEstate', 'id'],
                        ['realEstate', 'isNew'],
                        ['realEstate', 'luxury'],
                        ['realEstate', 'contract'],
                        ['seo', 'anchor'],
                        ['seo', 'url']
                    ],
                    errors='ignore'
                )

                # Rename columns to remove dots
                df.columns = df.columns.str.replace('.', '_')

                # Set index
                if 'realEstate_id' in df.columns:
                    df = df.set_index('realEstate_id')

                # Extract nested values
                if 'price' in df.columns:
                    df['price_value'] = df['price'].apply(lambda x: x.get('value') if isinstance(x, dict) else None)
                    df['price_priceRange'] = df['price'].apply(lambda x: x.get('priceRange') if isinstance(x, dict) else None)
                    df.drop('price', axis=1, inplace=True)

                if 'location' in df.columns:
                    df['location_city'] = df['location'].apply(lambda x: x.get('city') if isinstance(x, dict) else None)
                    df['location_latitude'] = df['location'].apply(lambda x: x.get('latitude') if isinstance(x, dict) else None)
                    df['location_longitude'] = df['location'].apply(
                        lambda x: x.get('longitude') if isinstance(x, dict) else None)
                    df['location_macrozone'] = df['location'].apply(
                        lambda x: x.get('macrozone') if isinstance(x, dict) else None)
                    df.drop('location', axis=1, inplace=True)

                # Apply data types
                data_types = {
                    'isNew': 'boolean',
                    'luxury': 'boolean',
                    'contract': 'category',
                    'category_name': 'category',
                    'ga4Condition': 'category',
                    'location_city': 'category',
                    'location_macrozone': 'category',
                    'price_priceRange': 'category',
                    'bathrooms': 'category',
                    'rooms': 'category',
                    'price_value': 'float'
                }

                for col, dtype in data_types.items():
                    if col in df.columns:
                        try:
                            df[col] = df[col].astype(dtype)
                        except:
                            continue

                return df, len(results), False, total_count, max_pages

            return pd.DataFrame(), 0, True, 0, 0

        except Exception as e:
            st.write(f"Error: {e}")
            time.sleep(delay)
            if attempt == retries - 1:
                return pd.DataFrame(), 0, True, 0, 0

    return pd.DataFrame(), 0, True, 0, 0

def fetch_all_pages(base_url, session, timeout_minutes=2):
    all_houses_df = pd.DataFrame()
    total_properties = 0
    start_time = time.time()
    max_pages = 80
    pages_scanned = 0
    start_run = True

    while True:
        if (time.time() - start_time) > (timeout_minutes * 60):
            st.warning("Timeout: ricerca interrotta per limite di tempo")
            break

        while max_pages >= 80:
            # Get first page to check total count and max pages
            first_page_url = f"{base_url}&pag=1"
            if start_run:
                df, count, fail, total_count, max_pages = read_page(first_page_url, session)
                st.info(f"Numero di annunci da caricare: {total_count} (in {max_pages} pagine)")
                start_run=False
                should_break = False  # Flag to control breaking out of all loops
            else:
                df, count, fail, _, max_pages = read_page(first_page_url, session)

            if fail or count == 0:
#                should_break = True
                break

            # Fetch pages in batches of 80
            batch_start = 1
            batch_end = min(80, max_pages)

            #st.info(f"Fetching pages {batch_start} to {batch_end}")

            # Fetch batch pages in parallel
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                for page in range(batch_start, batch_end + 1):
                    page_url = f"{base_url}&pag={page}"
                    futures.append(executor.submit(read_page, page_url, session))

                batch_results = []
                for future in as_completed(futures):
                    df, count, fail, _, _ = future.result()
                    if not fail and count > 0:
                        batch_results.append(df)
                    pages_scanned += 1

                if batch_results:
                    # Combine batch results
                    batch_df = pd.concat(batch_results, ignore_index=False)

                    # Update price range for next batch
                    if not batch_df.empty:
                        current_max_price = batch_df['price_value'].max()
                        new_base_url = re.sub(
                            r'prezzoMinimo=\d+',
                            f'prezzoMinimo={int(current_max_price) + 1}',
                            base_url
                        )
                        base_url = new_base_url

                    # Add batch to main DataFrame
                    all_houses_df = pd.concat([all_houses_df, batch_df])
                    total_properties = len(all_houses_df)

                if total_properties >= total_count: #pages_scanned >= max_pages:
                    should_break = True
                    break
                # reset number of pages scanned
                pages_scanned = 0

        if should_break:
            break

    # Remove duplicates
    all_houses_df = all_houses_df[~all_houses_df.index.duplicated(keep='first')]
    total_properties = len(all_houses_df)

    # Verify total count
    st.success(f"Recuparati n°{total_properties} annunci")
#    if total_properties < total_count:
#        st.warning(f"Salvati n°{len(all_houses_df)} annunci unici su {total_count}")

    return all_houses_df, total_properties

def get_search_url(filters):
    base_url = "https://www.immobiliare.it/api-next/search-list/listings"
    params = {
        "fkRegione": filters['regione'],
        "idProvincia": filters['provincia'],
        "idComune": filters['comune'],
        "idNazione": "IT",
        "idContratto": "1",
        "idCategoria": "1",
        "prezzoMinimo": filters['prezzoMinimo'],
        "prezzoMassimo": filters['prezzoMassimo'],
        "criterio": "prezzo",
        "ordine": "asc",
        "__lang": "it",
        "path" : "%2F"
    }

    search_url = f"{base_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
    print(search_url)
    return search_url

def create_filters(geodata):
    # Get unique regions for first dropdown
    regions = geodata[['region_id', 'region_label']].drop_duplicates()
    selected_region_label = st.selectbox("Region", regions['region_label'].unique())
    selected_region_id = regions[regions['region_label'] == selected_region_label]['region_id'].iloc[0]

    # Filter provinces based on selected region
    provinces = geodata[geodata['region_id'] == selected_region_id][['province_id', 'province_label']].drop_duplicates()
    selected_province_label = st.selectbox("Province", provinces['province_label'].unique())
    selected_province_id = provinces[provinces['province_label'] == selected_province_label]['province_id'].iloc[0]

    # Filter comuni based on selected province
    comuni = geodata[geodata['province_id'] == selected_province_id][['entity_id', 'entity_label']].drop_duplicates()
    selected_comune_label = st.selectbox("Comune", comuni['entity_label'].unique())
    selected_comune_id = comuni[comuni['entity_label'] == selected_comune_label]['entity_id'].iloc[0]

    # Price filters
    col1, col2 = st.columns(2)
    with col1:
        prezzoMinimo = st.number_input("Min Price", 0, 1000000, 50000)
    with col2:
        prezzoMassimo = st.number_input("Max Price", 0, 1000000, 500000)

    filters = {
        'regione': selected_region_id,
        'provincia': selected_province_id,
        'comune': selected_comune_id,
        'prezzoMinimo': prezzoMinimo,
        'prezzoMassimo': prezzoMassimo
    }

    return filters

def read_page_bak(url, session=""):

    def getp(mydict, prop):
        return mydict.get(prop, np.nan) if isinstance(mydict, dict) else np.nan

    if session == "":
        response = requests.get(url)
    else:
        response = session.get(url)

    if response.status_code == 200:
        data = json.loads(response.text)
        allproperties = data.get('results', {})

        seo_data = [allproperties[houseno].get('seo', {}) for houseno in range(len(allproperties))]
        realEstate_data = [allproperties[houseno].get('realEstate', {}) for houseno in range(len(allproperties))]
        properties = [realEstate_data[houseno].get('properties', [{}])[0] for houseno in range(len(allproperties))]

        houses_data = [
            {
                'id': getp(realEstate_data[houseno], 'id'),
                'isNew': getp(realEstate_data[houseno], 'isNew'),
                'luxury': getp(realEstate_data[houseno], 'luxury'),
                'contract': getp(realEstate_data[houseno], 'contract'),
                'description': getp(properties[houseno], 'description'),
                'condition': getp(properties[houseno], 'ga4Condition'),
                'floors': getp(properties[houseno], 'floor'),
                'garage': getp(properties[houseno], 'ga4Garage'),
                'surface': getp(properties[houseno], 'surface'),
                'bathrooms': getp(properties[houseno], 'bathrooms'),
                'rooms': getp(properties[houseno], 'rooms'),
                'saleType': getp(realEstate_data[houseno], 'type'),
                'anchor': getp(seo_data[houseno], 'anchor'),
                'category': getp(getp(properties[houseno], 'category'), 'name'),
                'heatingType': getp(getp(properties[houseno], 'energy'), 'ga4Heating'),
                'floor_value': getp(getp(properties[houseno], 'floor'), 'abbreviation'),
                'location_city': getp(getp(properties[houseno], 'location'), 'city'),
                'location_latitude': getp(getp(properties[houseno], 'location'), 'latitude'),
                'location_longitude': getp(getp(properties[houseno], 'location'), 'longitude'),
                'location_macrozone': getp(getp(properties[houseno], 'location'), 'macrozone'),
                'price_value': getp(getp(properties[houseno], 'price'), 'value'),
                'price_priceRange': getp(getp(properties[houseno], 'price'), 'priceRange'),
            }
            for houseno in range(len(allproperties))
        ]

        house_df = pd.DataFrame(houses_data).set_index('id')

        # Apply data types only to columns that exist
        data_types = {
            'isNew': 'boolean',
            'luxury': 'boolean',
            'contract': 'category',
            'category': 'category',
            'condition': 'category',
            'location_city': 'category',
            'location_macrozone': 'category',
            'price_priceRange': 'category',
            'saleType': 'category',
            'bathrooms': 'category',
            'rooms': 'category',
            'price_value': 'float'
        }

        # Apply types only to columns that exist in the DataFrame
        for col, dtype in data_types.items():
            if col in house_df.columns:
                try:
                    house_df[col] = house_df[col].astype(dtype)
                except:
                    continue

        countcheck = len(allproperties)
        fail = 0
        return house_df, countcheck, fail
    else:
        return pd.DataFrame(), 0, 1

def price_by_feature(houses_df, feature):
    # Price per m² by condition
    # Calculate average priceperm2 for each condition
    price_by_feature = houses_df.groupby(feature)['priceperm2'].mean().reset_index()

    # Create a DataFrame with the conditions and their average prices
    price_condition_df = pd.DataFrame({
        'Feature': price_by_feature[feature],
        'AveragePricePerM2': price_by_feature['priceperm2']
    })

    # Get n colors from viridis palette
    n_colors = price_condition_df['Feature'].nunique()
    viridis_colors = plotly.colors.sample_colorscale('viridis', n_colors)

    # Create color mapping dictionary
    color_map = {feature: color for feature, color in zip(price_condition_df['Feature'], viridis_colors)}

    # Update the figure
    fig = px.bar(
        price_condition_df,
        x='Feature',
        y='AveragePricePerM2',
        color='Feature',
        color_discrete_map=color_map
    )

    # Update layout
    fig.update_layout(
        showlegend=False,
        xaxis_title=feature,
        yaxis_title="Prezzo Medio per Metro Quadro (€)"
    )

    # Display the chart
    st.plotly_chart(fig, use_container_width=True)
