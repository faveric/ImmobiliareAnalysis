import streamlit as st
import requests
import json
import pandas as pd
import numpy as np
import geopandas as gpd
import folium
from streamlit_folium import folium_static


def getp(mydict, prop):
    return mydict.get(prop, np.nan) if isinstance(mydict, dict) else np.nan


def read_page(url, session=""):
    if session == "":
        response = requests.get(url)
    else:
        response = session.get(url)

    data_dtype = {
        'isNew': 'boolean',
        'luxury': 'boolean',
        'contract': 'category',
        'category': 'category',
        'description': 'str',
        'condition': 'category',
        'floor_value': 'object',
        'floors': 'str',
        'garage': 'str',
        'location_city': 'category',
        'location_latitude': 'str',
        'location_longitude': 'str',
        'location_macrozone': 'category',
        'price_value': 'float',
        'price_priceRange': 'category',
        'surface': 'str',
        'saleType': 'category',
        'bathrooms': 'category',
        'rooms': 'category',
    }

    countcheck = 0

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

        house_df = pd.DataFrame(houses_data).astype(data_dtype).set_index('id')
        countcheck = len(allproperties)
        fail = 0
        return house_df, countcheck, fail
    else:
        return pd.DataFrame(), 0, 1


def fetch_all_pages(base_url, session):
    all_houses_df = pd.DataFrame()
    page = 1
    total_properties = 0

    while True:
        page_url = f"{base_url}&pag={page}"
        df, count, fail = read_page(page_url, session)

        if fail or count == 0:
            break

        if not df.empty:
            all_houses_df = pd.concat([all_houses_df, df])
            total_properties += count
            #st.text(f"Fetched page {page} ({count} properties)")
            page += 1
        else:
            break

    return all_houses_df, total_properties

def get_search_url(filters):
    base_url = "https://www.immobiliare.it/api-next/search-list/real-estates/"
    params = {
        "fkRegione": "lom",
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

    return f"{base_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"


def main():
    st.title("Real Estate Analysis")

    # Import Geo data
    geodata = pd.read_csv('./geodata/geo_data.csv')

    print(geodata.columns)
    print(len(geodata))

    # Create columns for filters
    col1, col2 = st.columns(2)

    with col1:
        provincia = st.text_input("Provincia", "MI")
        comune = st.text_input("Codice Comune", 8081)
    with col2:
        prezzoMinimo = st.number_input("Min Price", 0, 1000000, 0)
        prezzoMassimo = st.number_input("Max Price", 0, 1000000, 1000000)


    filters = {
        'provincia': provincia,
        'comune': comune,
        'prezzoMinimo': prezzoMinimo,
        'prezzoMassimo': prezzoMassimo
    }

    if st.button("Search Properties"):
        with st.spinner("Fetching properties..."):
            url = get_search_url(filters)

            progress_text = st.empty()

            with requests.Session() as session:
                houses_df, total_properties = fetch_all_pages(url, session)

            if not houses_df.empty:
                st.success(f"Found {total_properties} properties")

                # Data processing
                houses_df['surface'] = houses_df['surface'].str.replace(' m²', '').astype(float)
                houses_df['priceperm2'] = houses_df['price_value'] / houses_df['surface']

                # Display statistics
                st.subheader("Statistics")
                col1, col2 = st.columns(2)

                with col1:
                    st.metric("Total Properties", len(houses_df))
                    st.metric("Avg Price", f"€{houses_df['price_value'].mean():,.0f}")
                    st.metric("Avg Surface", f"{houses_df['surface'].mean():.0f}m²")

                with col2:
                    st.metric("Median Price/m²", f"€{houses_df['priceperm2'].median():,.0f}")
                    st.metric("Min Price", f"€{houses_df['price_value'].min():,.0f}")
                    st.metric("Max Price", f"€{houses_df['price_value'].max():,.0f}")

                # Create map
                # Find first valid coordinates for map center
                st.subheader('Mappa delle proprietà')
                map_center = [45.4642, 9.1900]  # Default to Milan center
                zoom_start = 12  # Default zoom level

                # Get first valid coordinates from the dataframe
                valid_coords = houses_df[houses_df['location_latitude'].notna() &
                                         houses_df['location_longitude'].notna()]
                if not valid_coords.empty:
                    first_house = valid_coords.iloc[0]
                    map_center = [float(first_house['location_latitude']),
                                  float(first_house['location_longitude'])]

                m = folium.Map(location=map_center, zoom_start=zoom_start)

                for _, row in houses_df.iterrows():
                    if pd.notna(row['location_latitude']) and pd.notna(row['location_longitude']):
                        folium.CircleMarker(
                            location=[float(row['location_latitude']), float(row['location_longitude'])],
                            radius=8,
                            popup=f"Price: €{row['price_value']:,.0f}<br>"
                                  f"Surface: {row['surface']}m²<br>"
                                  f"Price/m²: €{row['priceperm2']:,.0f}<br>"
                                  f"Condition: {row['condition']}<br>"
                                  f"Heating: {row['heatingType']}",
                            color='red',
                            fill=True
                        ).add_to(m)

                folium_static(m)

                # Display additional statistics
                st.subheader("Numero di Proprietà per Condizione")
                st.bar_chart(houses_df['condition'].value_counts())

                # Convert the data into the right format for st.bar_chart
                st.subheader("Price Analysis")

                # Price per m² by condition
                condition_price = houses_df.groupby('condition')['priceperm2'].mean().round(0)
                st.subheader("Average Price per m² by Condition")
                st.bar_chart(condition_price)

                # Price per m² by bathrooms
                bathroom_price = houses_df.groupby('bathrooms')['priceperm2'].mean().round(0)
                st.subheader("Average Price per m² by Number of Bathrooms")
                st.bar_chart(bathroom_price)

                # Price per m² by rooms
                rooms_price = houses_df.groupby('rooms')['priceperm2'].mean().round(0)
                st.subheader("Average Price per m² by Number of Rooms")
                st.bar_chart(rooms_price)

                # Price per m² by garage
                garage_price = houses_df.groupby('garage')['priceperm2'].mean().round(0)
                st.subheader("Average Price per m² by Garage Type")
                st.bar_chart(garage_price)





if __name__ == "__main__":
    main()