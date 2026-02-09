from data_capture.ingestion import main

if __name__ == "__main__":
    main(SparkSession.builder.getOrCreate())
