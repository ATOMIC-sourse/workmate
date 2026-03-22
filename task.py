import csv
from abc import ABC, abstractmethod
from typing import Dict, List
from collections import defaultdict
from statistics import median
from tabulate import tabulate
import argparse



class DataCollector:
    def __init__(self):
        self.name: Dict[str, List[float]] = defaultdict(list)

    def read_csv_file(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            median_dict = {}
            for row in reader:
                name = row['student']
                if 'coffee_spent' in row and row['coffee_spent'].strip():
                    coffee = int(row['coffee_spent'])
                    self.name[name].append(coffee)
            for student, price in self.name.items():
                median_dict[student] = int(median(price))
            tab_items = [[student, price] for student, price in median_dict.items()]
            return tabulate(
                tab_items,
                headers=['students', 'price'], 
                tablefmt='grid',
                colalign=('left' ,'right'))

class BaseReport(ABC):
    
    def __init__(self):
        self.data = defaultdict(list)
    
    def read_csv_files(self, file_paths: List[str]) -> None:
        for file_path in file_paths:
            self._read_single_file(file_path)
    
    def _read_single_file(self, file_path: str) -> None:
        with open(file_path, 'r', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                if self._is_valid_row(row):
                    self._process_row(row)
    
    def _is_valid_row(self, row: Dict[str, str]) -> bool:
        return (
            'student' in row and 
            row['student'].strip() and
            'coffee_spent' in row and 
            row['coffee_spent'].strip()
        )
    
    def _process_row(self, row: Dict[str, str]) -> None:
        student = row['student'].strip()
        try:
            coffee = float(row['coffee_spent'])
            self.data[student].append(coffee)
        except ValueError:
            pass
    
    @abstractmethod
    def generate(self) -> List[List[any]]:
        pass
    
    @abstractmethod
    def get_headers(self) -> List[str]:
        pass
    
    @abstractmethod
    def get_tablefmt(self) -> str:
        return 'grid'
    
class MedianCoffeeReport(BaseReport):
    
    def generate(self) -> List[List[any]]:
        result = []
        for student, prices in self.data.items():
            if prices:
                median_price = median(prices)
                result.append([student, median_price])
        
        result.sort(key=lambda x: x[1], reverse=True)
        return result
    
    def get_headers(self) -> List[str]:
        return ['Student', 'Median Coffee Spent']
    
    def get_tablefmt(self) -> str:
        return 'grid'

    

class ReportFactory:
    
    _reports = {
        'median-coffee': MedianCoffeeReport,
    }
    
    @classmethod
    def create_report(cls, report_name: str) -> BaseReport:
        if report_name not in cls._reports:
            raise ValueError(f"Unknown report type: {report_name}")
        return cls._reports[report_name]()
    
    @classmethod
    def register_report(cls, name: str, report_class):
        cls._reports[name] = report_class




def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--files', nargs='+', required=True)
    parser.add_argument('-r', '--report', required=True)
    return parser.parse_args()

def main():
    collector = DataCollector()
    args = parse_arguments()
    for file_path in args.files:
        collector.read_csv_file(file_path)
    if collector.name:
        median_dict = {}
        for student, prices in collector.name.items():
            median_dict[student] = int(median(prices))
        
        tab_items = [[student, price] for student, price in median_dict.items()]
        print(tabulate(
            tab_items,
            headers=['students', 'price'],
            tablefmt='grid',
            colalign=('left', 'right')
        ))

if __name__ == '__main__':
    main()