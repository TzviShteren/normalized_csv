from app.service.data_handlers import process_global_terrorism, process_global_terrorism_second_csv
from dotenv import load_dotenv

load_dotenv(verbose=True)


def main():
    print("Starting batch publishing process...")
    process_global_terrorism()
    process_global_terrorism_second_csv()
    print("Finished publishing all data")


if __name__ == "__main__":
    main()
