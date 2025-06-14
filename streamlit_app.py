import streamlit as st
from pyspark.sql import SparkSession
import os, wget, tarfile
import subprocess


def setup_java():
    java_url = "https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.23+9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.23_9.tar.gz"
    
    # Use current working directory
    base_dir = os.getcwd()
    download_path = os.path.join(base_dir, "openjdk11.tar.gz")
    extract_root = os.path.join(base_dir, "java-11")

    if not os.path.exists(download_path):
        # st.write("Downloading Java 11...")
        wget.download(java_url, download_path)

    if not os.path.exists(extract_root):
        # st.write("Extracting Java 11...")
        os.makedirs(extract_root, exist_ok=True)
        with tarfile.open(download_path, "r:gz") as tar:
            tar.extractall(path=extract_root)

    extracted_dirs = os.listdir(extract_root)
    full_java_dir = os.path.join(extract_root, extracted_dirs[0])

    os.environ["JAVA_HOME"] = full_java_dir
    os.environ["PATH"] = f"{full_java_dir}/bin:" + os.environ["PATH"]

    subprocess.run([f"{full_java_dir}/bin/java", "-version"], check=True)

    return full_java_dir

# Call once
setup_java()


@st.cache_resource
def load_data():
    first_df = spark.read.parquet("./data/total_innings.parquet")
    sec_df = spark.read.parquet("./data/total_batting.parquet")

    first_df.createOrReplaceTempView("first_table")
    sec_df.createOrReplaceTempView("second_table")
    return first_df, sec_df

def run_sql_query(sql_file):
    '''This runs the sql query from the sql file and returns the result as a df'''
    with open(sql_file, "r") as file:
        query = file.read()
    return spark.sql(query)


# Set page configuration
st.set_page_config(
    page_title="Indian Planting League",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
    <style>
        body {
            background-color: #f8f9fa;
        }
        .main {
            background-color: #ffffff;
            border-radius: 15px;
            padding: 2rem;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .title {
            font-size: 3rem;
            color: #0d6efd;
            font-weight: bold;
        }
        .subtitle {
            font-size: 1.5rem;
            color: #6c757d;
        }
        .footer {
            font-size: 0.9rem;
            color: #adb5bd;
            margin-top: 2rem;
            text-align: center;
        }
        .button {
            background-color: #0d6efd;
            color: white;
            padding: 0.75rem 1.5rem;
            border-radius: 10px;
            font-weight: bold;
            text-decoration: none;
        }
    </style>
""", unsafe_allow_html=True)

icon_url_1 = "https://images.seeklogo.com/logo-png/53/1/tata-ipl-logo-png_seeklogo-531750.png"
col1, col2, col3 = st.columns([1, 3, 1])

with col1:
    st.image(icon_url_1, width=80)

with col2:
    st.markdown("<h2 style='text-align: center;'>Indian Planting League</h2>", unsafe_allow_html=True)


# Main container
with st.container():
    st.markdown('<div class="main">', unsafe_allow_html=True)
    
    st.markdown('<div class="title">Best Team of Indian Planting League (IPL) 2025 🏏</div>', unsafe_allow_html=True)
    
    # Create a Spark session
    spark = SparkSession.builder.appName('nestedJSON').getOrCreate()
    first_df, sec_df = load_data()

    st.subheader('Most plants planted at each Batting Position by a Single Player')

    result_df = run_sql_query("./data/sql/most_dot_balls_per_bat_position.sql")

    st.dataframe(result_df)
    st.markdown('<div class="footer">More data and analyses coming soon.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
