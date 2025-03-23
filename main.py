import streamlit as st
import requests
import pandas as pd
import folium
from streamlit_folium import folium_static
import plotly.express as px
from functions import read_page, fetch_all_pages, get_search_url, create_filters, price_by_feature
import seaborn as sns
import matplotlib.pyplot as plt

def main():
    # Initialize houses dataframe
    if 'houses_df_all' not in st.session_state:
        st.session_state['houses_df_all'] = pd.DataFrame()

    st.title("Analisi Mercato Immobiliare")
    st.write("Questo tool permette di analizzare il mercato immobiliare del comune selezionato consultando gli annunci online sul sito Immobiliare.it."
             "I dati sono aggiornati in tempo reale e vengono visualizzati in forma di grafici e mappe interattive."
             "Per iniziare, seleziona il comune di interesse, indica un range di prezzo e clicca su 'Avvia Ricerca'.")
    # Import Geo data
    geodata = pd.read_csv('./geodata/geo_data.csv')

    # Create filters
    filters = create_filters(geodata)

    if st.button("Avvia Ricerca"):
        with st.spinner("Recupero gli annunci..."):
            url = get_search_url(filters)

            progress_text = st.empty()

            with requests.Session() as session:
                st.session_state['houses_df_all'], total_properties = fetch_all_pages(url, session)

                # Data processing
                st.session_state['houses_df_all']['surface'] = st.session_state['houses_df_all']['surface'].str.replace(' m²', '').astype(float)
                st.session_state['houses_df_all']['priceperm2'] = st.session_state['houses_df_all']['price_value'] / st.session_state['houses_df_all']['surface']

    if not st.session_state['houses_df_all'].empty:
        aste = st.selectbox('Escludi Aste', ['Escludi', 'Includi'])
        aste_excluded = aste == 'Escludi'

        if aste_excluded:
            houses_df = st.session_state['houses_df_all'][st.session_state['houses_df_all']['realEstate_contract'] == 'sale'].copy()
            st.info(f'Il numero di annunci è stato ridotto a {len(houses_df)}')
        else:
            houses_df = st.session_state['houses_df_all'].copy()

        # Display statistics
        st.subheader("Statistics")
        col1, col2 = st.columns(2)

        with col1:
            st.metric("Numero Annunci", len(houses_df))
            st.metric("Prezzo Medio", f"€{houses_df['price_value'].mean():,.0f}")
            st.metric("Superficie Media", f"{houses_df['surface'].mean():.0f}m²")

        with col2:
            st.metric("Prezzo Mediano/m²", f"€{houses_df['priceperm2'].median():,.0f}")
            st.metric("Prezzo Minimo", f"€{houses_df['price_value'].min():,.0f}")
            st.metric("Prezzo Massimo", f"€{houses_df['price_value'].max():,.0f}")

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

        for _, row in houses_df.reset_index().iterrows():
            if pd.notna(row['location_latitude']) and pd.notna(row['location_longitude']):
                folium.CircleMarker(
                    location=[float(row['location_latitude']), float(row['location_longitude'])],
                    radius=8,
                    popup=f"ID: {row['realEstate_id']}<br>"
                          f"Price: €{row['price_value']:,.0f}<br>"
                          f"Surface: {row['surface']}m²<br>"
                          f"Price/m²: €{row['priceperm2']:,.0f}<br>"
                          f"Condition: {row['ga4Condition']}<br>"
                          f"Heating: {row['ga4Heating']}",
                    color='red',
                    fill=True
                ).add_to(m)

        folium_static(m)

        # Define the order and colors
        condition_order = ["Da ristrutturare", "Buono / Abitabile", "Ottimo / Ristrutturato",
                           "Nuovo / In costruzione"]
        viridis_colors = ["#440154", "#21908C", "#55C667", "#FDE725"]

        # Get the condition counts
        condition_counts = houses_df['ga4Condition'].value_counts()

        # Reorder the data according to your desired order
        # Create a DataFrame for better control
        condition_df = pd.DataFrame({
            'Condition': condition_counts.index,
            'Count': condition_counts.values
        })

        # Reorder based on your condition_order
        condition_df['Order'] = condition_df['Condition'].map(
            {cond: i for i, cond in enumerate(condition_order)})
        condition_df = condition_df.sort_values('Order')

        # Display additional statistics
        st.subheader("Numero di Proprietà per Condizione")

        # Create interactive Plotly chart
        fig = px.bar(
            condition_df,
            x='Condition',
            y='Count',
            color='Condition',
            color_discrete_map={cond: color for cond, color in zip(condition_order, viridis_colors)},
            category_orders={"Condition": condition_order}
        )

        # Update layout
        fig.update_layout(
            showlegend=False,
            xaxis_title="Condizione",
            yaxis_title="Numero di Proprietà",
            xaxis={'categoryorder': 'array', 'categoryarray': condition_order}
        )

        # Display the chart
        st.plotly_chart(fig, use_container_width=True)

        # Price per m² by condition
        # Calculate average priceperm2 for each condition
        price_by_condition = houses_df.groupby('ga4Condition')['priceperm2'].mean().reset_index()

        # Create a DataFrame with the conditions and their average prices
        price_condition_df = pd.DataFrame({
            'Condition': price_by_condition['ga4Condition'],
            'AveragePricePerM2': price_by_condition['priceperm2']
        })

        # Reorder based on your condition_order
        price_condition_df['Order'] = price_condition_df['Condition'].map(
            {cond: i for i, cond in enumerate(condition_order)})
        price_condition_df = price_condition_df.sort_values('Order')

        # Display chart title
        st.subheader("Prezzo Medio per Metro Quadro per Condizione")

        # Create interactive Plotly chart
        fig = px.bar(
            price_condition_df,
            x='Condition',
            y='AveragePricePerM2',
            color='Condition',
            color_discrete_map={cond: color for cond, color in zip(condition_order, viridis_colors)},
            category_orders={"Condition": condition_order}
        )

        # Update layout
        fig.update_layout(
            showlegend=False,
            xaxis_title="Condizione",
            yaxis_title="Prezzo Medio per Metro Quadro (€)",
            xaxis={'categoryorder': 'array', 'categoryarray': condition_order}
        )

        # Display the chart
        st.plotly_chart(fig, use_container_width=True)


        st.subheader("Prezzo Medio per Metro Quadro per Numero di Stanze")
        price_by_feature(houses_df, 'rooms')

        st.subheader("Prezzo Medio per Metro Quadro per Piano")
        price_by_feature(houses_df, 'floor_abbreviation')

        st.subheader("Prezzo Medio per Metro Quadro per Numero di Bagni")
        price_by_feature(houses_df, 'bathrooms')

        st.subheader("Prezzo Medio per Metro Quadro per Riscaldamento")
        price_by_feature(houses_df, 'ga4Heating')

        # Elenco delle proprietà
        st.subheader("Elenco delle proprietà")
        st.dataframe(houses_df)

if __name__ == "__main__":
    main()