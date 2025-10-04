import scraper
import parser
import combiner
import saver


def main():
    scraper.download()
    parser.parse_directory()
    combiner.add_weather()
    saver.save_to_postgres()
    saver.erase_raw_data()
    quit(0)



if __name__ == '__main__':
    main()