from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "emissions.raw"
    mlflow_tracking_uri: str = "http://localhost:5000"
    database_url: str = "postgresql+psycopg2://admin:admin123@localhost/emissions"

    class Config:
        env_file = ".env"


settings = Settings()
