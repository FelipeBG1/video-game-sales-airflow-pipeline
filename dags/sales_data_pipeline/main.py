from src.extract.load_data import load_data
from src.transform.transform_data import aggregate_data, clean_data
from src.load.save_data import save_data, save_to_db
from src.validate.data_quality import validate_clean_data, validate_aggregated_data
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

logger.info("Starting pipeline")

try:
    
    logger.info("Loading raw data")
    df = load_data()

    logger.info("Cleaning data")
    df_clean = clean_data(df)

    logger.info("Validating cleaned data")
    validate_clean_data(df_clean)

    logger.info("Aggregating data")
    df_agg = aggregate_data(df_clean)

    logger.info("Validating aggregated data")
    validate_aggregated_data(df_agg)

    logger.info("Saving CSV files")
    save_data(df_clean,'data/processed/clean_sales.csv')
    save_data(df_agg, 'data/processed/aggregate_sales.csv')

    logger.info("Saving data to PostgreSQL")
    save_to_db(df=df_clean, table='games_clean')
    save_to_db(df=df_agg, table='sales_by_platform_year')

    logger.info("Pipeline completed successfully")

except Exception as e:
    logger.exception(f"Pipeline failed: {e}")



