import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session
import altair as alt
import json
import traceback

# Set page config
st.set_page_config(
    page_title="🏀 Snow Bear Fan Experience Analytics",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Snowflake colors and styling
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #29B5E8 0%, #1E3A8A 100%);
        color: white !important;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
        margin-bottom: 30px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    
    .main-header h1, .main-header h3 {
        color: white !important;
    }
    
    .metric-card {
        background: white;
        padding: 20px;
        border-radius: 10px;
        border-left: 5px solid #29B5E8;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        margin: 10px 0;
    }
    
    .segment-card {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 15px;
        border-radius: 8px;
        margin: 5px 0;
        border-left: 4px solid #29B5E8;
    }
    
    .recommendation-box {
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 8px;
        padding: 15px;
        margin: 10px 0;
        border-left: 4px solid #28a745;
    }
    
    .theme-badge {
        background: #29B5E8;
        color: white;
        padding: 5px 10px;
        border-radius: 15px;
        font-size: 12px;
        margin: 2px;
        display: inline-block;
    }
    
    .snowflake-badge {
        background: #1E3A8A;
        color: white;
        padding: 5px 10px;
        border-radius: 15px;
        font-size: 12px;
        margin: 2px;
        display: inline-block;
    }
    
    /* Snowflake-themed widget styling */
    .stSelectbox > div > div > div {
        border-color: #29B5E8 !important;
    }
    
    /* Multiselect styling */
    .stMultiSelect > div > div > div {
        border-color: #29B5E8 !important;
    }
    
    .stMultiSelect [data-baseweb="tag"] {
        background-color: #29B5E8 !important;
        color: white !important;
    }
    
    /* Slider styling - ONLY the thumb, nothing else */
    .stSlider [role="slider"] {
        background-color: #29B5E8 !important;
        border-color: #29B5E8 !important;
    }
    
    .stButton > button {
        background-color: #29B5E8 !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
    }
    
    .stButton > button:hover {
        background-color: #1E3A8A !important;
        color: white !important;
    }
    
    /* Streamlit progress bar and other widgets */
    .stProgress > div > div > div > div {
        background-color: #29B5E8 !important;
    }
    
    /* Tabs styling - minimal blue theme without background */
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        color: #1E3A8A !important;
    }
    
    .stTabs [data-baseweb="tab-list"] button[aria-selected="true"] [data-testid="stMarkdownContainer"] p {
        color: #29B5E8 !important;
        font-weight: bold !important;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state for better management
def init_session_state():
    """Initialize session state variables"""
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    if 'df' not in st.session_state:
        st.session_state.df = None
    if 'themes_df' not in st.session_state:
        st.session_state.themes_df = None
    if 'session' not in st.session_state:
        st.session_state.session = None
    if 'error_count' not in st.session_state:
        st.session_state.error_count = 0
    if 'session_id' not in st.session_state:
        import uuid
        st.session_state.session_id = str(uuid.uuid4())[:8]  # Short unique ID

# Initialize session connection without caching
def get_snowflake_session():
    """Get Snowflake session with error handling"""
    try:
        if st.session_state.session is None:
            st.session_state.session = get_active_session()
        return st.session_state.session
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        return None

# Initialize session state
init_session_state()

# Get session first
session = get_snowflake_session()

if session is None:
    st.error("❌ Unable to connect to Snowflake. Please check your connection.")
    st.stop()

# Customer configuration - COMPATIBLE WITH QUICKSTART
DATABASE = session.get_current_database()
SCHEMA = "ANALYTICS"
CUSTOMER_SCHEMA = f"SNOW_BEAR_DB.ANALYTICS"
STAGE = "SEMANTIC_MODELS"

# Note: Data stored in SNOW_BEAR_DB schemas - hardcoded for quickstart compatibility

# Note: Query tags removed due to Snowflake native Streamlit restrictions
# Native Streamlit apps cannot modify session settings like query_tag

# Using Snowflake logo
logo_url = "https://logos-world.net/wp-content/uploads/2022/11/Snowflake-Symbol.png"
st.markdown(f"<div style='text-align: center;'><img src='{logo_url}' width='150'></div>", unsafe_allow_html=True)

# Header
st.markdown("""
<div class="main-header">
    <h1>❄️ Snow Bear Fan Experience Analytics</h1>
    <h3>Powered by Snowflake Cortex AI • From Arena to Insights</h3>
</div>
""", unsafe_allow_html=True)

# Sidebar
st.sidebar.title("🎯 Navigation")

# Load data function with better error handling and no caching decorator
def load_main_data():
    """Load main data with error handling"""
    try:
        if not st.session_state.data_loaded or st.session_state.df is None:
            with st.spinner("❄️ Loading Snow Bear fan data..."):
                query = f"""
                SELECT * FROM SNOW_BEAR_DB.GOLD_LAYER.QUALTRICS_SCORECARD
                ORDER BY REVIEW_DATE DESC
                LIMIT 10000
                """
                df = session.sql(query).to_pandas()
                st.session_state.df = df
                st.session_state.data_loaded = True
                
        return st.session_state.df
    except Exception as e:
        st.error(f"Error loading main data: {str(e)}")
        return pd.DataFrame()

def load_themes_data():
    """Load themes data with error handling"""
    try:
        if st.session_state.themes_df is None:
            query = f"""
            SELECT * FROM SNOW_BEAR_DB.GOLD_LAYER.EXTRACTED_THEMES_STRUCTURED
            ORDER BY THEME_NUMBER
            LIMIT 5000
            """
            themes_df = session.sql(query).to_pandas()
            st.session_state.themes_df = themes_df
                
        return st.session_state.themes_df
    except Exception as e:
        st.error(f"Error loading themes data: {str(e)}")
        return pd.DataFrame()

# Load data
try:
    df = load_main_data()
    themes_df = load_themes_data()
    
    if df.empty:
        st.error("❌ No data available. Please ensure the basketball survey data has been loaded and processed.")
        st.info("💡 Setup Instructions:\n1. Upload basketball survey data to the SNOW_BEAR_DATA_STAGE\n2. Run the Snow Bear analytics notebook\n3. Refresh this app")
        st.stop()
        
except Exception as e:
    st.error(f"❌ Failed to load data: {str(e)}")
    st.info("💡 Try refreshing the page or checking your Snowflake connection.")
    st.stop()

# Sidebar filters with error handling
st.sidebar.header("🔍 Filters")

# Date range filter with validation
try:
    min_date = df['REVIEW_DATE'].min()
    max_date = df['REVIEW_DATE'].max()
    
    if pd.isna(min_date) or pd.isna(max_date):
        min_date = datetime.now() - timedelta(days=30)
        max_date = datetime.now()
    
    date_range = st.sidebar.date_input(
        "Review Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        help="Select the date range for fan reviews"
    )
except Exception as e:
    st.sidebar.error(f"Error setting up date filter: {str(e)}")
    date_range = (datetime.now() - timedelta(days=30), datetime.now())

# Handle single date selection
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range if hasattr(date_range, '__iter__') else date_range

# Other filters with error handling
try:
    segments = df['SEGMENT'].dropna().unique().tolist()
    selected_segments = st.sidebar.multiselect("Fan Segments", segments, default=segments[:5] if len(segments) > 5 else segments)
    
    themes = df['MAIN_THEME'].dropna().unique().tolist()
    selected_themes = st.sidebar.multiselect("Main Themes", themes)
    
    sentiment_range = st.sidebar.slider("Sentiment Range", -1.0, 1.0, (-1.0, 1.0), 0.1)
    score_range = st.sidebar.slider("Satisfaction Score", 1, 5, (1, 5))
except Exception as e:
    st.sidebar.error(f"Error setting up filters: {str(e)}")
    selected_segments = []
    selected_themes = []
    sentiment_range = (-1.0, 1.0)
    score_range = (1, 5)

# Filter data with error handling
try:
    filtered_df = df[
        (df['SEGMENT'].isin(selected_segments) if selected_segments else True) &
        (df['AGGREGATE_SENTIMENT'].between(sentiment_range[0], sentiment_range[1])) &
        (df['AGGREGATE_SCORE'].between(score_range[0], score_range[1])) &
        (df['REVIEW_DATE'] >= pd.to_datetime(start_date)) &
        (df['REVIEW_DATE'] <= pd.to_datetime(end_date))
    ]

    if selected_themes:
        filtered_df = filtered_df[filtered_df['MAIN_THEME'].isin(selected_themes)]
        
except Exception as e:
    st.error(f"Error filtering data: {str(e)}")
    filtered_df = df.copy()

# Add data refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.session_state.data_loaded = False
    st.session_state.df = None
    st.session_state.themes_df = None
    st.rerun()

# Clear cache button for troubleshooting
if st.sidebar.button("🗑️ Clear Cache"):
    st.cache_data.clear()
    st.session_state.clear()
    st.success("Cache cleared! Please refresh the page.")

# Tabs
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📊 Executive Dashboard", 
    "👥 Fan Journey Explorer", 
    "💭 Sentiment Deep Dive", 
    "🎯 Theme & Segment Analysis", 
    "🚀 Recommendation Engine",
    "🔍 Interactive Search",
    "🧠 AI Assistant"
])

# Tab 1: Executive Dashboard with error handling
with tab1:
    st.header("📊 Executive Dashboard")
    
    try:
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        # Check if we have data after filtering
        if len(filtered_df) == 0:
            st.warning("⚠️ No data matches your current filters. Please adjust your filter criteria.")
            
            # Show empty metrics
            with col1:
                st.metric("Average Fan Score", "N/A", delta="No data")
            with col2:
                st.metric("Average Sentiment", "N/A", delta="No data")
            with col3:
                st.metric("Total Fans", "0", delta="No data")
            with col4:
                st.metric("Satisfaction Rate", "N/A", delta="No data")
        else:
            with col1:
                avg_score = filtered_df['AGGREGATE_SCORE'].mean()
                st.metric("Average Fan Score", f"{avg_score:.1f}/5", delta=f"{avg_score-3:.1f} vs neutral")
            
            with col2:
                avg_sentiment = filtered_df['AGGREGATE_SENTIMENT'].mean()
                st.metric("Average Sentiment", f"{avg_sentiment:.2f}", delta=f"{avg_sentiment:.2f} vs neutral")
            
            with col3:
                total_fans = len(filtered_df)
                st.metric("Total Fans", f"{total_fans:,}", delta=f"{total_fans}")
            
            with col4:
                satisfaction_rate = len(filtered_df[filtered_df['AGGREGATE_SCORE'] >= 4]) / len(filtered_df) * 100
                st.metric("Satisfaction Rate", f"{satisfaction_rate:.1f}%", delta=f"{satisfaction_rate-70:.1f}% vs target")
        
        # Visualizations with error handling
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 Fan Score Distribution")
            if not filtered_df.empty:
                score_counts = filtered_df['AGGREGATE_SCORE'].value_counts().sort_index()
                score_df = pd.DataFrame({'Score': score_counts.index, 'Count': score_counts.values})
                
                chart = alt.Chart(score_df).mark_bar(color='#29B5E8').encode(
                    x=alt.X('Score:O', title='Score'),
                    y=alt.Y('Count:Q', title='Number of Fans'),
                    tooltip=['Score', 'Count']
                ).properties(
                    title='Fan Satisfaction Scores',
                    width=300,
                    height=250
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No data to display")
        
        with col2:
            st.subheader("🎯 Sentiment vs Score")
            if not filtered_df.empty and len(filtered_df) < 1000:  # Limit data size for performance
                scatter_chart = alt.Chart(filtered_df.head(500)).mark_circle(size=60).encode(
                    x=alt.X('AGGREGATE_SENTIMENT:Q', title='Sentiment'),
                    y=alt.Y('AGGREGATE_SCORE:Q', title='Score'),
                    color=alt.Color('SEGMENT:N', title='Segment'),
                    tooltip=['SEGMENT', 'MAIN_THEME', 'SECONDARY_THEME', 'AGGREGATE_SENTIMENT', 'AGGREGATE_SCORE']
                ).properties(
                    title='Sentiment vs Satisfaction Score',
                    width=300,
                    height=250
                )
                st.altair_chart(scatter_chart, use_container_width=True)
            else:
                st.info("Chart not available - too much data or no data to display")
        
        # Segment breakdown
        st.subheader("👥 Fan Segment Analysis")
        if not filtered_df.empty:
            segment_analysis = filtered_df.groupby('SEGMENT').agg({
                'AGGREGATE_SCORE': 'mean',
                'AGGREGATE_SENTIMENT': 'mean',
                'ID': 'count'
            }).round(2)
            segment_analysis.columns = ['Avg Score', 'Avg Sentiment', 'Fan Count']
            segment_analysis['Satisfaction %'] = (filtered_df.groupby('SEGMENT')['AGGREGATE_SCORE'].apply(lambda x: (x >= 4).mean() * 100)).round(1)
            
            st.dataframe(segment_analysis, use_container_width=True)
        else:
            st.info("No segment data to display")
            
    except Exception as e:
        st.error(f"Error in Executive Dashboard: {str(e)}")
        st.info("💡 Try refreshing the data or adjusting your filters.")

# Tab 2: Fan Journey Explorer with better error handling
with tab2:
    st.header("👥 Fan Journey Explorer")
    
    try:
        # Check if we have fans in filtered data
        if len(filtered_df) == 0:
            st.warning("⚠️ No fans match your current filters. Please adjust your filter criteria to explore individual fan journeys.")
        else:
            # Individual fan selector - only show fans that exist in filtered data
            available_fans = filtered_df['ID'].unique()[:100]  # Limit to first 100 for performance
            selected_fan = st.selectbox("Select a Fan to Explore", available_fans)
            
            # Get fan data safely
            fan_matches = filtered_df[filtered_df['ID'] == selected_fan]
            if len(fan_matches) == 0:
                st.error("Selected fan not found in filtered data. Please select a different fan.")
            else:
                fan_data = fan_matches.iloc[0]
                
                # Fan Profile Section
                st.subheader(f"🎭 Fan Profile: {selected_fan}")
                
                # Fan summary
                st.markdown(f"""
                <div class="segment-card">
                    <h4>📋 Fan Summary</h4>
                    <p><strong>Segment:</strong> {fan_data.get('SEGMENT', 'N/A')}</p>
                    <p><strong>Alt Segment:</strong> {fan_data.get('SEGMENT_ALT', 'N/A')}</p>
                    <p><strong>Overall Score:</strong> {fan_data.get('AGGREGATE_SCORE', 'N/A')}/5</p>
                    <p><strong>Overall Sentiment:</strong> {fan_data.get('AGGREGATE_SENTIMENT', 0):.2f}</p>
                    <p><strong>Main Theme:</strong> <span class="theme-badge">{fan_data.get('MAIN_THEME', 'N/A')}</span></p>
                    <p><strong>Secondary Theme:</strong> <span class="snowflake-badge">{fan_data.get('SECONDARY_THEME', 'N/A')}</span></p>
                </div>
                """, unsafe_allow_html=True)
                
                # Experience breakdown - Stadium/Event categories
                st.subheader("🏀 Experience Breakdown")
                experience_categories = ['FOOD_OFFERING', 'GAME_EXPERIENCE', 'MERCHANDISE_OFFERING', 
                                       'MERCHANDISE_PRICING', 'OVERALL_EVENT', 'PARKING',
                                       'SEAT_LOCATION', 'STADIUM_ACCESS']
                
                # Create metrics display
                metric_cols = st.columns(3)
                for i, cat in enumerate(experience_categories):
                    if f'{cat}_SCORE' in fan_data and f'{cat}_SENTIMENT' in fan_data:
                        col_idx = i % 3
                        score = fan_data.get(f'{cat}_SCORE', 'N/A')
                        sentiment = fan_data.get(f'{cat}_SENTIMENT', 0)
                        
                        with metric_cols[col_idx]:
                            # Convert category name to display format
                            display_name = cat.replace('_', ' ').title()
                            if cat == 'FOOD_OFFERING':
                                display_name = "Food & Concessions"
                            elif cat == 'GAME_EXPERIENCE':
                                display_name = "Game Experience"
                            elif cat == 'MERCHANDISE_OFFERING':
                                display_name = "Merchandise Quality"
                            elif cat == 'MERCHANDISE_PRICING':
                                display_name = "Merchandise Pricing"
                            elif cat == 'OVERALL_EVENT':
                                display_name = "Overall Event"
                            elif cat == 'STADIUM_ACCESS':
                                display_name = "Stadium Access"
                            
                            st.metric(
                                label=display_name,
                                value=f"{score}/5",
                                delta=f"Sentiment: {sentiment:.2f}"
                            )
                
                # Fan comments section
                st.subheader("💬 Fan Comments")
                comments = {
                    'Food & Concessions': fan_data.get('FOOD_OFFERING_COMMENT'),
                    'Game Experience': fan_data.get('GAME_EXPERIENCE_COMMENT'),
                    'Merchandise Quality': fan_data.get('MERCHANDISE_OFFERING_COMMENT'),
                    'Merchandise Pricing': fan_data.get('MERCHANDISE_PRICING_COMMENT'),
                    'Overall Event': fan_data.get('OVERALL_EVENT_COMMENT'),
                    'Parking': fan_data.get('PARKING_COMMENT'),
                    'Seat Location': fan_data.get('SEAT_LOCATION_COMMENT'),
                    'Stadium Access': fan_data.get('STADIUM_COMMENT')
                }
                
                # Display comments in a more visible way
                comment_cols = st.columns(2)
                comment_idx = 0
                for category, comment in comments.items():
                    if pd.notna(comment) and comment:
                        with comment_cols[comment_idx % 2]:
                            st.markdown(f"""
                            <div class="segment-card">
                                <h5>💬 {category}</h5>
                                <p>{comment}</p>
                            </div>
                            """, unsafe_allow_html=True)
                            comment_idx += 1
                
                # Recommendations section
                st.subheader("💡 Recommendations")
                
                # Display recommendations
                if pd.notna(fan_data.get('BUSINESS_RECOMMENDATION')):
                    st.markdown(f"""
                    <div class="recommendation-box">
                        <h5>💼 Business Recommendation</h5>
                        <p>{fan_data.get('BUSINESS_RECOMMENDATION')}</p>
                    </div>
                    """, unsafe_allow_html=True)
                
                if pd.notna(fan_data.get('COMPLEX_RECOMMENDATION')):
                    st.markdown(f"""
                    <div class="recommendation-box">
                        <h5>🧠 Complex Recommendation</h5>
                        <p>{fan_data.get('COMPLEX_RECOMMENDATION')}</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
    except Exception as e:
        st.error(f"Error in Fan Journey Explorer: {str(e)}")
        st.info("💡 Try selecting a different fan or refreshing the data.")

# Tab 3: Sentiment Deep Dive with error handling
with tab3:
    st.header("💭 Sentiment Deep Dive")
    
    try:
        if filtered_df.empty:
            st.warning("⚠️ No data available for sentiment analysis. Please adjust your filters.")
        else:
            # Sentiment heatmap
            st.subheader("🔥 Sentiment Analysis by Category")
            
            sentiment_cols = [col for col in ['FOOD_OFFERING_SENTIMENT', 'GAME_EXPERIENCE_SENTIMENT', 
                             'MERCHANDISE_OFFERING_SENTIMENT', 'MERCHANDISE_PRICING_SENTIMENT',
                             'OVERALL_EVENT_SENTIMENT', 'PARKING_SENTIMENT',
                             'SEAT_LOCATION_SENTIMENT', 'STADIUM_ACCESS_SENTIMENT'] if col in filtered_df.columns]
            
            if sentiment_cols:
                sentiment_data = filtered_df[sentiment_cols].mean().to_frame('Average Sentiment')
                sentiment_data.index = [col.replace('_SENTIMENT', '').replace('_', ' ').title() for col in sentiment_cols]
                
                # Convert to DataFrame for altair
                sentiment_chart_df = sentiment_data.reset_index()
                sentiment_chart_df.columns = ['Category', 'Average_Sentiment']
                
                sentiment_bar = alt.Chart(sentiment_chart_df).mark_bar().encode(
                    x=alt.X('Category:N', title='Category'),
                    y=alt.Y('Average_Sentiment:Q', title='Average Sentiment'),
                    color=alt.Color('Average_Sentiment:Q', scale=alt.Scale(scheme='blues'), title='Sentiment'),
                    tooltip=['Category', 'Average_Sentiment']
                ).properties(
                    title='Average Sentiment by Category',
                    height=300
                )
                st.altair_chart(sentiment_bar, use_container_width=True)
            
            # Sentiment over time and distribution
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📅 Sentiment Trends")
                if 'REVIEW_DATE' in filtered_df.columns:
                    daily_sentiment = filtered_df.groupby('REVIEW_DATE')['AGGREGATE_SENTIMENT'].mean().reset_index()
                    
                    if not daily_sentiment.empty and len(daily_sentiment) > 1:
                        trend_chart = alt.Chart(daily_sentiment).mark_line(color='#29B5E8').encode(
                            x=alt.X('REVIEW_DATE:T', title='Date'),
                            y=alt.Y('AGGREGATE_SENTIMENT:Q', title='Average Sentiment'),
                            tooltip=['REVIEW_DATE', 'AGGREGATE_SENTIMENT']
                        ).properties(
                            title='Daily Sentiment Trends',
                            height=250
                        )
                        st.altair_chart(trend_chart, use_container_width=True)
                    else:
                        st.info("Not enough data for trend analysis")
                else:
                    st.info("Date column not available")
            
            with col2:
                st.subheader("🎯 Sentiment Distribution")
                hist_chart = alt.Chart(filtered_df.head(1000)).mark_bar(color='#29B5E8').encode(
                    alt.X('AGGREGATE_SENTIMENT:Q', bin=alt.Bin(maxbins=20), title='Sentiment'),
                    y=alt.Y('count()', title='Count'),
                    tooltip=['count()']
                ).properties(
                    title='Sentiment Distribution',
                    height=250
                )
                st.altair_chart(hist_chart, use_container_width=True)
            
            # Sentiment by segment - simplified as table
            st.subheader("👥 Sentiment by Segment")
            if sentiment_cols:
                segment_sentiment = filtered_df.groupby('SEGMENT')[sentiment_cols].mean().round(2)
                st.dataframe(segment_sentiment, use_container_width=True)
            else:
                st.info("Sentiment columns not available")
                
    except Exception as e:
        st.error(f"Error in Sentiment Deep Dive: {str(e)}")
        st.info("💡 Try adjusting your filters or refreshing the data.")

# Continue with simplified versions of remaining tabs...
# Tab 4: Theme & Segment Analysis
with tab4:
    st.header("🎯 Theme & Segment Analysis")
    
    try:
        if filtered_df.empty:
            st.warning("⚠️ No data available for analysis. Please adjust your filters.")
        else:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🏷️ Theme Distribution")
                if 'MAIN_THEME' in filtered_df.columns:
                    theme_counts = filtered_df['MAIN_THEME'].value_counts().head(10)
                    
                    if not theme_counts.empty:
                        theme_df = pd.DataFrame({'Theme': theme_counts.index, 'Count': theme_counts.values})
                        theme_chart = alt.Chart(theme_df).mark_bar(color='#29B5E8').encode(
                            x=alt.X('Count:Q', title='Count'),
                            y=alt.Y('Theme:N', sort='-x', title='Theme'),
                            tooltip=['Theme', 'Count']
                        ).properties(
                            title='Primary Themes Distribution',
                            height=300
                        )
                        st.altair_chart(theme_chart, use_container_width=True)
                    else:
                        st.info("No theme data available")
                else:
                    st.info("Theme column not available")
            
            with col2:
                st.subheader("👥 Segment Distribution")
                if 'SEGMENT' in filtered_df.columns:
                    segment_counts = filtered_df['SEGMENT'].value_counts().head(10)
                    
                    if not segment_counts.empty:
                        segment_df = pd.DataFrame({'Segment': segment_counts.index, 'Count': segment_counts.values})
                        segment_chart = alt.Chart(segment_df).mark_bar(color='#1E3A8A').encode(
                            x=alt.X('Count:Q', title='Count'),
                            y=alt.Y('Segment:N', sort='-x', title='Segment'),
                            tooltip=['Segment', 'Count']
                        ).properties(
                            title='Fan Segments Distribution',
                            height=300
                        )
                        st.altair_chart(segment_chart, use_container_width=True)
                    else:
                        st.info("No segment data available")
                else:
                    st.info("Segment column not available")
            
            # Theme analysis
            st.subheader("📊 Theme Performance Analysis")
            if 'MAIN_THEME' in filtered_df.columns and not filtered_df.empty:
                theme_analysis = filtered_df.groupby('MAIN_THEME').agg({
                    'AGGREGATE_SCORE': ['mean', 'count'],
                    'AGGREGATE_SENTIMENT': 'mean'
                }).round(2)
                
                theme_analysis.columns = ['Avg Score', 'Count', 'Avg Sentiment']
                theme_analysis['Satisfaction Rate %'] = (filtered_df.groupby('MAIN_THEME')['AGGREGATE_SCORE'].apply(lambda x: (x >= 4).mean() * 100)).round(1)
                
                st.dataframe(theme_analysis.sort_values('Avg Score', ascending=False).head(10), use_container_width=True)
            else:
                st.info("No theme performance data available")
            
    except Exception as e:
        st.error(f"Error in Theme & Segment Analysis: {str(e)}")
        st.info("💡 Try adjusting your filters or refreshing the data.")

# Simplified remaining tabs for performance and stability
with tab5:
    st.header("🚀 Recommendation Engine")
    st.info("💡 Recommendation engine simplified for better performance")
    
    try:
        if not filtered_df.empty:
            # Simple recommendation analysis
            if 'BUSINESS_RECOMMENDATION' in filtered_df.columns:
                business_recs = filtered_df['BUSINESS_RECOMMENDATION'].dropna()
                st.metric("Business Recommendations Available", len(business_recs))
                
                if not business_recs.empty:
                    st.subheader("📝 Sample Business Recommendations")
                    for i, rec in enumerate(business_recs.head(3).values):
                        with st.expander(f"Recommendation #{i+1}"):
                            st.write(rec)
            else:
                st.info("No recommendation data available")
        else:
            st.warning("No data available. Please adjust your filters.")
    except Exception as e:
        st.error(f"Error in Recommendation Engine: {str(e)}")

with tab6:
    st.header("🔍 Interactive Search & Explore")
    
    try:
        # Cortex Search Service functionality
        with st.form("search_form"):
            search_term = st.text_input("🔍 Search fan comments with AI", placeholder="e.g., 'game experience', 'parking issues', 'food quality'")
            search_submitted = st.form_submit_button("🔍 AI Search")
        
        if search_submitted and search_term:
            with st.spinner("🤖 AI-powered search analyzing fan comments..."):
                try:
                    # Use the SNOWBEAR_SEARCH_ANALYSIS Cortex Search Service - Updated for DataOps
                    search_query = f"""
                    WITH search_results AS (
                        SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                            'SNOW_BEAR_DB.ANALYTICS.SNOWBEAR_SEARCH_ANALYSIS',
                            '{{
                                "query": "{search_term}",
                                "columns":[
                                    "aggregate_comment",
                                    "aggregate_score",
                                    "segment",
                                    "segment_alt",
                                    "main_theme",
                                    "secondary_theme",
                                    "game_experience_score",
                                    "overall_event_score",
                                    "parking_score",
                                    "food_offering_score",
                                    "id"
                                ],
                                "limit": 200
                            }}'
                        ) as search_result
                    )
                    SELECT 
                        result.value:aggregate_comment::string as aggregate_comment,
                        result.value:aggregate_score::string as aggregate_score,
                        result.value:segment::string as segment,
                        result.value:segment_alt::string as segment_alt,
                        result.value:main_theme::string as main_theme,
                        result.value:secondary_theme::string as secondary_theme,
                        result.value:game_experience_score::string as game_experience_score,
                        result.value:overall_event_score::string as overall_event_score,
                        result.value:parking_score::string as parking_score,
                        result.value:food_offering_score::string as food_offering_score,
                        result.value:id::string as fan_id,
                        result.index + 1 as relevance_rank
                    FROM search_results,
                    LATERAL FLATTEN(input => PARSE_JSON(search_results.search_result):results) as result
                    ORDER BY result.index
                    """
                    
                    search_results_df = session.sql(search_query).to_pandas()
                    
                    if not search_results_df.empty:
                        st.success(f"🎯 Found {len(search_results_df)} AI-powered results for '{search_term}'")
                        
                        # Show search insights
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            avg_score = pd.to_numeric(search_results_df['AGGREGATE_SCORE'], errors='coerce').mean()
                            st.metric("Avg Score", f"{avg_score:.1f}/5" if not pd.isna(avg_score) else "N/A")
                        with col2:
                            top_theme = search_results_df['MAIN_THEME'].mode().iloc[0] if not search_results_df['MAIN_THEME'].empty else "N/A"
                            st.metric("Top Theme", top_theme)
                        with col3:
                            top_segment = search_results_df['SEGMENT'].mode().iloc[0] if not search_results_df['SEGMENT'].empty else "N/A"
                            st.metric("Top Segment", top_segment)
                        
                        st.markdown("---")
                        
                        # Display results with better formatting
                        for idx, row in search_results_df.iterrows():
                            with st.expander(f"🏀 Fan {row.get('FAN_ID', 'Unknown')} - {row.get('SEGMENT', 'Unknown')} - Score: {row.get('AGGREGATE_SCORE', 'N/A')}/5 (Rank #{row.get('RELEVANCE_RANK', idx+1)})"):
                                
                                # Comment section
                                st.markdown("### 💬 Fan Comment")
                                st.markdown(f"*{row.get('AGGREGATE_COMMENT', 'No comment available')}*")
                                
                                # Themes and scores
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.markdown("**🏷️ Themes**")
                                    st.markdown(f"• **Main:** {row.get('MAIN_THEME', 'Unknown')}")
                                    st.markdown(f"• **Secondary:** {row.get('SECONDARY_THEME', 'Unknown')}")
                                    st.markdown(f"• **Segment Alt:** {row.get('SEGMENT_ALT', 'Unknown')}")
                                
                                with col2:
                                    st.markdown("**📊 Category Scores**")
                                    scores = {
                                        'Game Experience': row.get('GAME_EXPERIENCE_SCORE'),
                                        'Overall Event': row.get('OVERALL_EVENT_SCORE'),
                                        'Parking': row.get('PARKING_SCORE'),
                                        'Food Offering': row.get('FOOD_OFFERING_SCORE')
                                    }
                                    
                                    for category, score in scores.items():
                                        if score and score != 'null':
                                            st.markdown(f"• **{category}:** {score}/5")
                    else:
                        st.info(f"🔍 No results found for '{search_term}'. Try different search terms like 'parking', 'food quality', or 'game atmosphere'.")
                        
                        # Show example searches
                        st.markdown("### 💡 Try These Search Examples:")
                        example_cols = st.columns(3)
                        with example_cols[0]:
                            st.markdown("**Experience Issues:**")
                            st.markdown("• parking problems")
                            st.markdown("• long lines")
                            st.markdown("• crowded concourse")
                        with example_cols[1]:
                            st.markdown("**Food & Concessions:**")
                            st.markdown("• food quality")
                            st.markdown("• expensive concessions")
                            st.markdown("• drink options")
                        with example_cols[2]:
                            st.markdown("**Game Atmosphere:**")
                            st.markdown("• exciting game")
                            st.markdown("• crowd energy")
                            st.markdown("• entertainment value")
                        
                except Exception as e:
                    st.error(f"Error with AI search: {str(e)}")
                    st.info("💡 Make sure the SNOWBEAR_SEARCH_ANALYSIS service is created and accessible")
                    
                    # Fallback to basic search
                    st.markdown("### 🔄 Falling back to basic search...")
                    if not filtered_df.empty and 'AGGREGATE_COMMENT' in filtered_df.columns:
                        basic_results = filtered_df[
                            filtered_df['AGGREGATE_COMMENT'].str.contains(search_term, case=False, na=False)
                        ]
                        
                        if not basic_results.empty:
                            st.info(f"Found {len(basic_results)} basic results")
                            for idx, row in basic_results.head(3).iterrows():
                                with st.expander(f"Fan {row.get('ID', 'Unknown')} - Basic Result"):
                                    st.markdown(f"**Comment:** {row.get('AGGREGATE_COMMENT', 'No comment')}")
                        else:
                            st.info("No basic results found either.")
        
        # Quick search buttons
        st.markdown("### ⚡ Quick Searches")
        quick_cols = st.columns(4)
        quick_searches = ["parking issues", "food quality", "game experience", "crowd atmosphere"]
        
        for i, quick_term in enumerate(quick_searches):
            with quick_cols[i]:
                if st.button(f"🔍 {quick_term}", key=f"quick_{i}"):
                    # This would trigger a search - in a real app you'd handle this with session state
                    st.info(f"Click the search button above and enter: '{quick_term}'")
                    
    except Exception as e:
        st.error(f"Error in Interactive Search: {str(e)}")
        st.info("💡 Try refreshing the page or check your search service setup")

    # --- AI Narrative (Cortex Complete) over last Analyst result ---
    if st.session_state.get("sb_last_df") is not None:
        st.markdown("---")
        st.subheader("🤖 AI Narrative (Cortex Complete)")
        colx1, colx2, colx3 = st.columns([2,1,1])
        with colx1:
            cc_model = st.selectbox("Model", ["llama3.1-8b","llama3.1-70b","mixtral-8x7b","mistral-large"], index=0, key="sb_cc_model")
        with colx2:
            cc_temp = st.slider("Temp", 0.0, 1.0, 0.2, 0.1, key="sb_cc_temp")
        with colx3:
            cc_tokens = st.number_input("Max Tokens", min_value=200, max_value=8000, value=1500, step=100, key="sb_cc_tokens")
        prompt_cc = st.text_area("Optional question for the AI about these results (leave blank for general analysis)", value="", height=80, key="sb_cc_prompt")
        if st.button("Generate Narrative", key="sb_cc_run"):
            try:
                sample = json.loads(st.session_state["sb_last_df"].head(50).to_json(orient="records", date_format="iso") )
                context_obj = {
                    "columns": list(st.session_state["sb_last_df"].columns),
                    "row_count": int(len(st.session_state["sb_last_df"])),
                    "sample_rows": sample,
                    "original_sql": st.session_state.get("sb_last_sql", "")
                }
                context_json = json.dumps(context_obj, ensure_ascii=False).replace("'","''")
                user_text = (prompt_cc or "Provide an executive-style analysis: key trends, outliers, comparisons, and recommended next steps. Use bullet points where helpful.").replace("'","''")
                cc_sql = f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    '{cc_model}',
                    [
                        {{'role':'system','content':'You are a senior data analyst. Be concise and insightful.'}},
                        {{'role':'user','content':'Context JSON: {context_json}\n\nQuestion: {user_text}'}}
                    ],
                    {{'temperature': {cc_temp}, 'max_tokens': {int(cc_tokens)}}}
                ):choices[0]:messages::string AS AI_RESPONSE
                """
                cc_df = session.sql(cc_sql).to_pandas()
                if not cc_df.empty:
                    st.markdown(cc_df.iloc[0]["AI_RESPONSE"])
                else:
                    st.info("No AI response.")
            except Exception as e:
                st.error(f"Cortex Complete error: {e}")

# Tab 7: AI Assistant
with tab7:
    st.header("🧠 AI Assistant - Cortex Analyst")
    st.markdown("*Ask questions about your fan data in natural language*")
    
    # Get available semantic models
    cmd = f"""ls @{CUSTOMER_SCHEMA}.{STAGE}"""
    try:
        semantic_files = session.sql(cmd).collect()
        list_files = []
        for semantic_file in semantic_files:
            filename = semantic_file["name"]
            # Remove stage name prefix if present (e.g., "semantic_models/file.yaml" -> "file.yaml")
            if "/" in filename:
                filename = filename.split("/")[-1]
            list_files.append(filename)
        
        if not list_files:
            st.error("No semantic models found in the stage. Please create semantic models first.")
            st.stop()
    except Exception as e:
        st.error(f"Error accessing semantic models: {e}")
        st.info("💡 Note: Make sure semantic models are uploaded to the stage")
        list_files = ["snow_bear_fan_360.yaml"]  # Default fallback
    
    # Semantic model selection with session state
    if 'selected_semantic_model' not in st.session_state:
        st.session_state.selected_semantic_model = list_files[0] if list_files else None
    
    FILE = st.selectbox('Choose your semantic model', list_files, 
                       index=list_files.index(st.session_state.selected_semantic_model) if st.session_state.selected_semantic_model in list_files else 0,
                       key='semantic_model_selector')
    
    # Update session state when selection changes
    if FILE != st.session_state.selected_semantic_model:
        st.session_state.selected_semantic_model = FILE
        # Clear chat history when semantic model changes
        if "analyst_messages" in st.session_state:
            st.session_state.analyst_messages = []
        st.success(f"✅ Semantic model changed to: {FILE}")
        st.info("💡 Chat history cleared. You can now ask questions using the new semantic model.")
    
    st.info(f"Using semantic model: `{FILE}`")
    
    # Cortex Analyst Integration
    with st.form("analyst_form"):
        col1, col2 = st.columns([3, 1])
        
        with col1:
            analyst_query = st.text_area("Ask your Snowbear fan data anything:", 
                                       placeholder="e.g., 'What are the main drivers of fan satisfaction?', 'How does food offerings drive satisfaction?', 'Which segments have the highest engagement?'",
                                       height=100)
        
        with col2:
            st.markdown("**💡 Example Questions:**")
            st.markdown("*Click to populate the text area*")
            example_questions = [
                "What drives fan satisfaction?",
                "Streaming quality vs overall sentiment?",
                "Which segments are most engaged?",
                "App experience impact on scores?",
                "Pricing sensitivity by segment?"
            ]
            
            for i, question in enumerate(example_questions):
                st.markdown(f"🏀 {question}")
            
            st.markdown("*Type your question above and click 'Ask AI Assistant'*")
        
        analyst_submitted = st.form_submit_button("🤖 Ask AI Assistant")
    
    # Initialize chat history for this tab
    if "analyst_messages" not in st.session_state:
        st.session_state.analyst_messages = []
    
    # Helper function to call Cortex Analyst API
    def send_analyst_message(prompt: str, semantic_model: str) -> dict:
        """Send a message to Cortex Analyst API and return the response."""
        try:
            import _snowflake
            
            request_body = {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ]
                    }
                ],
                "semantic_model_file": semantic_model,
            }
            
            resp = _snowflake.send_snow_api_request(
                "POST",
                f"/api/v2/cortex/analyst/message",
                {},
                {},
                request_body,
                {},
                30000,
            )
            
            if resp["status"] < 400:
                return json.loads(resp["content"])
            else:
                raise Exception(f"API request failed with status {resp['status']}: {resp}")
                
        except Exception as e:
            st.error(f"Error calling Cortex Analyst API: {e}")
            return None

    # Process form submission
    if analyst_submitted and analyst_query:
        with st.spinner("AI Assistant is analyzing your fan data..."):
            try:
                # Call Cortex Analyst API with proper semantic model
                semantic_model = f"@{CUSTOMER_SCHEMA}.{STAGE}/{FILE}"
                response = send_analyst_message(analyst_query, semantic_model)
                
                if response and "message" in response:
                    content = response["message"]["content"]
                    
                    # Add to chat history
                    st.session_state.analyst_messages.append({
                        "role": "user", 
                        "content": [{"type": "text", "text": analyst_query}]
                    })
                    st.session_state.analyst_messages.append({
                        "role": "assistant", 
                        "content": content
                    })
                    
                    # Store latest response for persistence
                    st.session_state["latest_analyst_response"] = content
                    st.session_state["show_success_message"] = True
                else:
                    st.error("No valid response from Cortex Analyst API.")
                    st.info("💡 Note: Make sure your Snow Bear semantic model is created and accessible")
                    
            except Exception as e:
                st.error(f"AI Assistant error: {str(e)}")
                st.info("💡 Note: Make sure your Snow Bear semantic model is created and accessible")
    
    # Display persisted success message
    if st.session_state.get("show_success_message") and st.session_state.get("latest_analyst_response"):
        st.success("🎯 AI Analysis Complete!")
    
    # Display the latest AI response (persists across all interactions)
    if st.session_state.get("latest_analyst_response"):
        st.markdown("---")
        st.markdown("### 📋 Latest AI Analysis")
        
        content = st.session_state["latest_analyst_response"]
        for item in content:
            if item["type"] == "text":
                st.markdown("#### 📊 AI Analysis")
                st.markdown(item["text"])
            
            elif item["type"] == "sql":
                generated_sql = item["statement"]
                # Collapsible SQL (auto-collapsed)
                with st.expander("🔍 Generated SQL", expanded=False):
                    st.code(generated_sql, language="sql")

                # Execute the generated SQL ONCE if not already executed
                if analyst_submitted and analyst_query:
                    with st.spinner("Executing generated SQL..."):
                        try:
                            results_df = session.sql(generated_sql.strip(";")).to_pandas()
                            # Persist for explore section below
                            st.session_state["sb_last_sql"] = generated_sql.strip(";")
                            st.session_state["sb_last_df"] = results_df
                            if not results_df.empty:
                                st.session_state["sql_success_message"] = f"✅ Query executed successfully! {len(results_df)} rows. See 'Explore Result' below for charts and data."
                            else:
                                st.session_state["sql_success_message"] = "Query executed but returned no results."
                        except Exception as e:
                            st.session_state["sql_success_message"] = f"Error executing generated SQL: {e}"
                
                # Display persisted SQL execution message
                if st.session_state.get("sql_success_message"):
                    if "successfully" in st.session_state["sql_success_message"]:
                        st.success(st.session_state["sql_success_message"])
                    elif "Error" in st.session_state["sql_success_message"]:
                        st.error(st.session_state["sql_success_message"])
                    else:
                        st.warning(st.session_state["sql_success_message"])
            
            elif item["type"] == "suggestions":
                st.markdown("#### 💡 Suggestions")
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"persist_suggestion_{suggestion_index}"):
                        analyst_query = suggestion
                        st.rerun()
    
    # Interactive visuals and AI narrative for the last Analyst result
    if st.session_state.get("sb_last_df") is not None:
        st.markdown("---")
        st.subheader("🔎 Explore Result (Data/Bar/Line)")
        last_df = st.session_state["sb_last_df"]
        view = st.radio("View", ["Data", "Bar", "Line"], horizontal=True, key="sb_view")

        # Identify numeric vs others
        columns = list(last_df.columns)
        numeric_cols = [c for c in columns if pd.api.types.is_numeric_dtype(last_df[c])]
        non_numeric_cols = [c for c in columns if c not in numeric_cols]

        def apply_grain(df: pd.DataFrame, col: str, grain: str) -> pd.DataFrame:
            if col not in df.columns:
                return df
            try:
                dt = pd.to_datetime(df[col], errors="coerce")
                if dt.notna().sum() == 0:
                    return df
                if grain == "day":
                    df[col] = dt.dt.to_period('D').dt.to_timestamp()
                elif grain == "quarter":
                    df[col] = dt.dt.to_period('Q').dt.to_timestamp()
                elif grain == "year":
                    df[col] = dt.dt.to_period('Y').dt.to_timestamp()
                else:
                    df[col] = dt.dt.to_period('M').dt.to_timestamp()
            except Exception:
                pass
            return df

        if view == "Data":
            st.dataframe(last_df, use_container_width=True)

        elif view == "Bar":
            c1, c2, c3 = st.columns([2,1,1])
            with c1:
                bar_dim = st.selectbox("Dimension", options=non_numeric_cols or columns, key="sb_bar_dim")
            with c2:
                stacked = st.checkbox("Stacked", value=True, key="sb_bar_stacked")
            # Grain for time-like
            with c3:
                grain = st.selectbox("Grain", ["month","day","quarter","year"], index=0, key="sb_bar_grain") if bar_dim in columns else None

            series = st.multiselect("Series (metrics)", options=numeric_cols, default=numeric_cols[:2], key="sb_bar_series")
            if series:
                df_plot = last_df[[bar_dim] + series].copy() if bar_dim in last_df.columns else last_df[series].copy()
                if grain and bar_dim in df_plot.columns:
                    df_plot = apply_grain(df_plot, bar_dim, grain)

                # If time-like, create discrete label and order for proper side-by-side grouping
                use_label = False
                if bar_dim in df_plot.columns and pd.api.types.is_datetime64_any_dtype(df_plot[bar_dim]):
                    try:
                        ts = pd.to_datetime(df_plot[bar_dim], errors="coerce")
                        if grain == "day":
                            x_label = ts.dt.strftime('%b %d %Y')
                        elif grain == "quarter":
                            x_label = ts.dt.to_period('Q').astype(str)
                        elif grain == "year":
                            x_label = ts.dt.strftime('%Y')
                        else:
                            x_label = ts.dt.strftime('%b %Y')
                        df_plot["X_LABEL"] = x_label
                        df_plot["X_ORDER"] = ts.view('int64')
                        use_label = True
                    except Exception:
                        use_label = False

                id_vars = []
                if bar_dim in df_plot.columns:
                    id_vars.append(bar_dim)
                if use_label:
                    id_vars.extend(["X_LABEL", "X_ORDER"])  
                long_df = df_plot.melt(id_vars=id_vars, value_vars=series, var_name="Series", value_name="Value")
                if use_label:
                    long_df = long_df.sort_values("X_ORDER")
                    x_enc = alt.X("X_LABEL:N", sort=None, title=bar_dim)
                else:
                    x_enc = alt.X(f"{bar_dim}:{'T' if (bar_dim in df_plot.columns and pd.api.types.is_datetime64_any_dtype(df_plot[bar_dim])) else 'N'}") if bar_dim in df_plot.columns else alt.X("Series:N")
                if stacked:
                    chart = alt.Chart(long_df).mark_bar().encode(x=x_enc, y=alt.Y("Value:Q", stack="zero"), color="Series:N", tooltip=[bar_dim if bar_dim in df_plot.columns else "Series", "Value:Q"]).properties(height=420)
                else:
                    chart = alt.Chart(long_df).mark_bar().encode(x=x_enc, y=alt.Y("Value:Q", stack=None), color="Series:N", xOffset="Series:N", tooltip=[(alt.Tooltip("X_LABEL:N", title=bar_dim) if use_label else alt.Tooltip(f"{bar_dim}:{'T' if (bar_dim in df_plot.columns and pd.api.types.is_datetime64_any_dtype(df_plot[bar_dim])) else 'N'}", title=bar_dim)) if bar_dim in df_plot.columns else alt.Tooltip("Series:N"), "Value:Q"]).properties(height=420)
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("Select at least one numeric metric.")

        elif view == "Line":
            c1, c2, c3 = st.columns([2,2,1])
            with c1:
                line_dim = st.selectbox("Dimension", options=columns, key="sb_line_dim")
            with c2:
                primary = st.multiselect("Primary series (metrics)", options=numeric_cols, default=numeric_cols[:2], key="sb_line_primary")
            with c3:
                grain = st.selectbox("Grain", ["month","day","quarter","year"], index=0, key="sb_line_grain")
            secondary = st.selectbox("Secondary (optional, RHS)", options=["(none)"] + [c for c in numeric_cols if c not in primary], key="sb_line_secondary")

            if primary:
                df_plot = last_df.copy()
                if grain:
                    df_plot = apply_grain(df_plot, line_dim, grain)
                # primary layer
                melt_cols = [line_dim] + primary
                if line_dim not in df_plot.columns:
                    st.warning("Selected dimension not in results; showing data only.")
                    st.dataframe(last_df, use_container_width=True)
                else:
                    df_primary = df_plot[melt_cols].copy()
                    for m in primary:
                        df_primary[m] = pd.to_numeric(df_primary[m], errors="coerce")
                    long_p = df_primary.melt(id_vars=[line_dim], value_vars=primary, var_name="Series", value_name="Value")
                    x_type = "T" if pd.api.types.is_datetime64_any_dtype(df_plot[line_dim]) else "N"
                    layer_primary = alt.Chart(long_p).mark_line(point=True).encode(x=alt.X(f"{line_dim}:{x_type}"), y=alt.Y("Value:Q", title="Value"), color="Series:N", tooltip=[line_dim, "Series", "Value:Q"]).properties(height=420)
                    if secondary and secondary != "(none)":
                        df_sec = df_plot[[line_dim, secondary]].copy()
                        df_sec[secondary] = pd.to_numeric(df_sec[secondary], errors="coerce")
                        layer_secondary = alt.Chart(df_sec).mark_line(point=True, color="#ef4444").encode(x=alt.X(f"{line_dim}:{x_type}"), y=alt.Y(f"{secondary}:Q", axis=alt.Axis(title=secondary, titleColor="#ef4444")), tooltip=[line_dim, alt.Tooltip(f"{secondary}:Q")])
                        st.altair_chart(alt.layer(layer_primary, layer_secondary).resolve_scale(y='independent'), use_container_width=True)
                    else:
                        st.altair_chart(layer_primary, use_container_width=True)

    # Cortex Complete narrative over last Analyst result
    if st.session_state.get("sb_last_df") is not None:
        st.markdown("---")
        st.subheader("🤖 AI Narrative (Cortex Complete)")
        colx1, colx2, colx3 = st.columns([2,1,1])
        with colx1:
            cc_model = st.selectbox("Model", ["llama3.1-8b","llama3.1-70b","mixtral-8x7b","mistral-large"], index=0, key="sb_cc_model_tab7")
        with colx2:
            cc_temp = st.slider("Temp", 0.0, 1.0, 0.2, 0.1, key="sb_cc_temp_tab7")
        with colx3:
            cc_tokens = st.number_input("Max Tokens", min_value=200, max_value=8000, value=1500, step=100, key="sb_cc_tokens_tab7")
        prompt_cc = st.text_area("Optional question for the AI about these results (leave blank for general analysis)", value="", height=80, key="sb_cc_prompt_tab7")
        if st.button("Generate Narrative", key="sb_cc_run_tab7"):
            try:
                sample = json.loads(st.session_state["sb_last_df"].head(50).to_json(orient="records", date_format="iso") )
                context_obj = {
                    "columns": list(st.session_state["sb_last_df"].columns),
                    "row_count": int(len(st.session_state["sb_last_df"])),
                    "sample_rows": sample,
                    "original_sql": st.session_state.get("sb_last_sql", "")
                }
                context_json = json.dumps(context_obj, ensure_ascii=False).replace("'","''")
                user_text = (prompt_cc or "Provide an executive-style analysis: key trends, outliers, comparisons, and recommended next steps. Use bullet points where helpful.").replace("'","''")
                cc_sql = f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    '{cc_model}',
                    [
                        {{'role':'system','content':'You are a senior data analyst. Be concise and insightful.'}},
                        {{'role':'user','content':'Context JSON: {context_json}\n\nQuestion: {user_text}'}}
                    ],
                    {{'temperature': {cc_temp}, 'max_tokens': {int(cc_tokens)}}}
                ):choices[0]:messages::string AS AI_RESPONSE
                """
                cc_df = session.sql(cc_sql).to_pandas()
                if not cc_df.empty:
                    narrative_response = cc_df.iloc[0]["AI_RESPONSE"]
                    st.session_state["latest_narrative_tab7"] = narrative_response
                    st.markdown(narrative_response)
                else:
                    st.info("No AI response.")
            except Exception as e:
                st.error(f"Cortex Complete error: {e}")
        
        # Display persisted narrative if available
        elif st.session_state.get("latest_narrative_tab7"):
            st.markdown("#### 📌 Previous AI Narrative:")
            st.markdown(st.session_state["latest_narrative_tab7"])

    # Display chat history
    if st.session_state.analyst_messages:
        st.markdown("### 💬 Conversation History")
        for message_index, message in enumerate(st.session_state.analyst_messages):
            # Use container instead of chat_message for compatibility
            message_container = st.container()
            with message_container:
                role_emoji = "🙋‍♂️" if message["role"] == "user" else "🤖"
                st.markdown(f"**{role_emoji} {message['role'].title()}:**")
                
                if message["role"] == "user":
                    st.markdown(message["content"][0]["text"])
                else:
                    # Display assistant content
                    for item in message["content"]:
                        if item["type"] == "text":
                            st.markdown(item["text"])
                        elif item["type"] == "sql":
                            with st.expander("SQL Query", expanded=False):
                                st.code(item["statement"], language="sql")
    
    # Quick Insights Panel
    st.subheader("⚡ Quick Insights")
    
    insight_cols = st.columns(3)
    
    with insight_cols[0]:
        if st.button("🎯 Top Pain Points"):
            with st.spinner("Analyzing..."):
                try:
                    pain_points_query = f"""
                    SELECT MAIN_THEME, AVG(AGGREGATE_SCORE) as avg_score, COUNT(*) as count
                    FROM SNOW_BEAR_DB.ANALYTICS.QUALTRICS_SCORECARD
                    WHERE AGGREGATE_SCORE <= 2
                    GROUP BY MAIN_THEME
                    ORDER BY count DESC
                    LIMIT 5
                    """
                    pain_points = session.sql(pain_points_query).to_pandas()
                    
                    st.markdown("**🔴 Top Pain Points:**")
                    for idx, row in pain_points.iterrows():
                        st.markdown(f"• **{row['MAIN_THEME']}** - {row['COUNT']} fans (avg: {row['AVG_SCORE']:.1f}/5)")
                        
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    with insight_cols[1]:
        if st.button("🌟 Top Highlights"):
            with st.spinner("Analyzing..."):
                try:
                    highlights_query = f"""
                    SELECT MAIN_THEME, AVG(AGGREGATE_SCORE) as avg_score, COUNT(*) as count
                    FROM SNOW_BEAR_DB.ANALYTICS.QUALTRICS_SCORECARD
                    WHERE AGGREGATE_SCORE >= 4
                    GROUP BY MAIN_THEME
                    ORDER BY count DESC
                    LIMIT 5
                    """
                    highlights = session.sql(highlights_query).to_pandas()
                    
                    st.markdown("**🟢 Top Highlights:**")
                    for idx, row in highlights.iterrows():
                        st.markdown(f"• **{row['MAIN_THEME']}** - {row['COUNT']} fans (avg: {row['AVG_SCORE']:.1f}/5)")
                        
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    with insight_cols[2]:
        if st.button("📈 Segment Insights"):
            with st.spinner("Analyzing..."):
                try:
                    segment_query = f"""
                    SELECT SEGMENT, AVG(AGGREGATE_SCORE) as avg_score, 
                           AVG(AGGREGATE_SENTIMENT) as avg_sentiment,
                           COUNT(*) as count
                    FROM SNOW_BEAR_DB.ANALYTICS.QUALTRICS_SCORECARD
                    GROUP BY SEGMENT
                    ORDER BY avg_score DESC
                    """
                    segments = session.sql(segment_query).to_pandas()
                    
                    st.markdown("**👥 Segment Performance:**")
                    for idx, row in segments.iterrows():
                        st.markdown(f"• **{row['SEGMENT']}** - {row['COUNT']} fans")
                        st.markdown(f"  Score: {row['AVG_SCORE']:.1f}/5, Sentiment: {row['AVG_SENTIMENT']:.2f}")
                        
                except Exception as e:
                    st.error(f"Error: {str(e)}")

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #6c757d; padding: 20px;">
    <p>❄️ Snow Bear Fan Experience Analytics • Powered by Snowflake Cortex AI</p>
    <p>From Arena to Insights in a Blink of an Eye</p>
</div>
""", unsafe_allow_html=True)

# Debug information
if st.sidebar.checkbox("🔧 Show Debug Info"):
    st.sidebar.markdown("### Debug Information")
    st.sidebar.markdown(f"**Data Shape:** {df.shape if not df.empty else 'No data'}")
    st.sidebar.markdown(f"**Filtered Shape:** {filtered_df.shape if not filtered_df.empty else 'No filtered data'}")
    st.sidebar.markdown(f"**Error Count:** {st.session_state.error_count}")