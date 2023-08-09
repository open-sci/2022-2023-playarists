# Import Streamlit
#import streamlit as st

# Use Streamlit's magic commands to write "Hello, World!"
#st.write("Hello, World!")

import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import numpy as np

# Load data
country_df = pd.read_csv('countries.csv')

# First Viz
def viz1():
    total_erih_plus = 11128
    total_oc_meta = 8689
    coverage_percentage = (total_oc_meta / total_erih_plus) * 100
    remaining = 100 - coverage_percentage
    coverage_data = [coverage_percentage, remaining]
    labels = ['Covered in OpenCitations Meta', 'Not in OpenCitations Meta']
    plt.title('ERIH Plus Journals in OC Meta Coverage')
    sns.set_style("whitegrid")
    plt.pie(coverage_data, labels=labels, autopct='%1.1f%%', startangle=90)
    plt.axis('equal')
    st.pyplot(plt)

# Second Viz
def viz2():
    meta_coverage_df = pd.read_csv('meta_coverage.csv')  # replace '<your file>' with your actual file path
    access_counts = meta_coverage_df['Open Access'].value_counts()
    plt.figure(figsize=(6, 6))
    sns.set_style("whitegrid")
    plt.pie(access_counts, labels=access_counts.index, autopct='%1.1f%%')
    plt.title('Open Access Status')
    st.pyplot(plt)

# Third Viz
def viz3():
    color_scale = [
        [0.0, '#f7fbff'],
        [0.001, '#deebf7'],
        [0.002, '#c6dbef'],
        [0.005, '#9ecae1'],
        [0.10, '#6baed6'],
        [0.20, '#4292c6'],
        [0.30, '#2171b5'],
        [0.40, '#08519c'],
        [0.50, '#08306b'],
        [1.0, '#081d58'],
    ]
    fig = px.choropleth(country_df, locations='Country',
                        locationmode='country names',
                        color='Publication_count',
                        hover_name='Country',
                        color_continuous_scale=color_scale,
                        title='Publications by Country')
    st.plotly_chart(fig)

# Fourth viz
def viz4():
    all_countries = country_df.sort_values(by='Publication_count', ascending=False)
    countries = all_countries.iloc[:30]
    plt.figure(figsize=(10, 8))
    sns.barplot(x='Publication_count', y='Country', data=countries, palette='flare')
    plt.xscale('log')
    plt.xlabel('Publication Count')
    plt.ylabel('Countries')
    plt.title('Publications by Country (Top 30)')
    plt.grid(axis="x", linewidth=0.2)
    x_values = np.array([100, 1000, 10000, 100000, 1000000])
    plt.xticks(x_values, x_values)

    st.pyplot(plt)
    
# Fifth viz
def viz5():
    all_countries = country_df.sort_values(by='Journal_count', ascending=False)
    countries = all_countries.iloc[:30]
    plt.figure(figsize=(10, 8))
    sns.barplot(x='Journal_count', y='Country', data=countries, palette='flare')
    plt.xlabel('Journal Count')
    plt.ylabel('Countries')
    plt.title('Journals by Country (Top 30)')
    plt.grid(axis="x", linewidth=0.2)

    st.pyplot(plt)

# Sixth viz
def viz6():
    all_countries = country_df.sort_values(by='Publication_count', ascending=False)
    last_countries = all_countries.iloc[-30:]
    plt.figure(figsize=(10, 8))
    sns.barplot(x='Publication_count', y='Country', data=last_countries, palette='viridis')
    plt.xlabel('Publication Count')
    plt.ylabel('Countries')
    plt.title('Publications by Country  (Last 30)')
    plt.grid(axis="x", linewidth=0.2)

    st.pyplot(plt)
    
# Seventh viz
def viz7():
    all_countries = country_df.sort_values(by='Journal_count', ascending=False)
    last_countries = all_countries.iloc[-30:]
    plt.figure(figsize=(10, 8))
    sns.barplot(x='Journal_count', y='Country', data=last_countries, palette='viridis')
    plt.xlabel('Journal Count')
    plt.ylabel('Countries')
    plt.title('Journals by Country (Last 30)')
    plt.grid(axis="x", linewidth=0.2)

    st.pyplot(plt)

# Eight viz
def viz8():
    disciplines = pd.read_csv('disciplines.csv')  # replace '<your file>' with your actual file path

    colors = ['steelblue', 'skyblue', 'lightskyblue', 'deepskyblue', 'dodgerblue', 'cornflowerblue',
              'mediumblue', 'royalblue', 'mediumslateblue', 'slateblue', 'blueviolet', 'darkviolet',
              'mediumorchid', 'indigo', 'purple', 'darkmagenta', 'palegreen', 'limegreen', 'forestgreen',
              'darkgreen', 'gold', 'goldenrod', 'darkorange', 'chocolate', 'sienna', 'saddlebrown',
              'tomato', 'orangered', 'firebrick', 'crimson', 'maroon']

    sns.set()  # Imposta le impostazioni predefinite di seaborn

    plt.figure(figsize=(10, 6))
    sns.barplot(data=disciplines, x='Publication_count', y='Discipline', palette=colors)
    plt.xlabel('Publications')
    plt.ylabel('Discipline')
    plt.title('Publications by Disciplines')
    plt.grid(axis="x", linewidth=0.2)

    st.pyplot(plt)

    
# Streamlit code
def main():
    st.title("Visualizations")
    st.header("ERIH Plus Journals in OC Meta Coverage")
    viz1()
    st.header("Open Access Status")
    viz2()
    st.header("Publications by Country")
    viz3()
    st.header("Publications by Country Top 30")
    viz4()
    st.header("Journals by Country Top 30")
    viz5()
    st.header("Publications by Country Last 30")
    viz6()
    st.header("Journals by Country Last 30")
    viz7()
    st.header("Publications by Discipline")
    viz8()


if __name__ == "__main__":
    main()
