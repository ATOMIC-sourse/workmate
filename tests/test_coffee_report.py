import pytest
import tempfile
import csv
import os
from unittest.mock import Mock, patch, MagicMock
from collections import defaultdict
from statistics import median

# Импортируем классы из основного скрипта
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from task import (
    DataCollector, BaseReport, MedianCoffeeReport, 
    ReportFactory, parse_arguments, main
)


class TestDataCollector:
    """Тесты для DataCollector"""
    
    @pytest.fixture
    def sample_csv_file(self):
        """Создание временного CSV файла с тестовыми данными"""
        data = [
            ['student', 'date', 'coffee_spent', 'sleep_hours', 'study_hours', 'mood', 'exam'],
            ['Алексей Смирнов', '2024-06-01', '450', '4.5', '12', 'норм', 'Математика'],
            ['Алексей Смирнов', '2024-06-02', '500', '4.0', '14', 'устал', 'Математика'],
            ['Алексей Смирнов', '2024-06-03', '550', '3.5', '16', 'зомби', 'Математика'],
            ['Дарья Петрова', '2024-06-01', '200', '7.0', '6', 'отл', 'Математика'],
            ['Дарья Петрова', '2024-06-02', '250', '6.5', '8', 'норм', 'Математика'],
            ['Дарья Петрова', '2024-06-03', '300', '6.0', '9', 'норм', 'Математика'],
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(data)
            temp_file = f.name
        
        yield temp_file
        os.unlink(temp_file)
    
    @pytest.fixture
    def csv_with_empty_values(self):
        """Создание CSV файла с пустыми значениями"""
        data = [
            ['student', 'date', 'coffee_spent', 'sleep_hours', 'study_hours', 'mood', 'exam'],
            ['Алексей Смирнов', '2024-06-01', '', '4.5', '12', 'норм', 'Математика'],
            ['Дарья Петрова', '2024-06-01', '200', '7.0', '6', 'отл', 'Математика'],
            ['', '2024-06-01', '300', '7.0', '6', 'отл', 'Математика'],
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(data)
            temp_file = f.name
        
        yield temp_file
        os.unlink(temp_file)
    
    @pytest.fixture
    def csv_with_invalid_values(self):
        """Создание CSV файла с некорректными значениями"""
        data = [
            ['student', 'date', 'coffee_spent', 'sleep_hours', 'study_hours', 'mood', 'exam'],
            ['Алексей Смирнов', '2024-06-01', 'invalid', '4.5', '12', 'норм', 'Математика'],
            ['Дарья Петрова', '2024-06-01', '200', '7.0', '6', 'отл', 'Математика'],
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(data)
            temp_file = f.name
        
        yield temp_file
        os.unlink(temp_file)
    
    def test_read_csv_file_valid_data(self, sample_csv_file):
        """Тест чтения валидных данных из CSV"""
        collector = DataCollector()
        collector.read_csv_file(sample_csv_file)
        
        assert len(collector.name) == 2
        assert 'Алексей Смирнов' in collector.name
        assert 'Дарья Петрова' in collector.name
        assert collector.name['Алексей Смирнов'] == [450, 500, 550]
        assert collector.name['Дарья Петрова'] == [200, 250, 300]
    
    def test_read_csv_file_empty_values(self, csv_with_empty_values):
        """Тест обработки пустых значений"""
        collector = DataCollector()
        collector.read_csv_file(csv_with_empty_values)
        
        # Алексей с пустым значением не должен быть добавлен
        assert 'Алексей Смирнов' not in collector.name
        # Дарья с валидным значением должна быть добавлена
        assert 'Дарья Петрова' in collector.name
        assert collector.name['Дарья Петрова'] == [200]
        # Пустое имя студента не должно быть добавлено
        assert '' not in collector.name
    
    def test_read_csv_file_invalid_values(self, csv_with_invalid_values):
        """Тест обработки некорректных значений"""
        collector = DataCollector()
        collector.read_csv_file(csv_with_invalid_values)
        
        # Алексей с invalid значением не должен быть добавлен
        assert 'Алексей Смирнов' not in collector.name
        # Дарья с валидным значением должна быть добавлена
        assert 'Дарья Петрова' in collector.name
        assert collector.name['Дарья Петрова'] == [200]
    
    def test_read_csv_file_multiple_calls(self, sample_csv_file):
        """Тест многократного чтения файлов"""
        collector = DataCollector()
        collector.read_csv_file(sample_csv_file)
        collector.read_csv_file(sample_csv_file)
        
        # Данные должны накапливаться
        assert len(collector.name['Алексей Смирнов']) == 6
        assert collector.name['Алексей Смирнов'] == [450, 500, 550, 450, 500, 550]
    
    def test_read_csv_file_missing_columns(self):
        """Тест чтения файла с отсутствующими колонками"""
        data = [
            ['student', 'date', 'sleep_hours'],
            ['Алексей Смирнов', '2024-06-01', '4.5'],
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(data)
            temp_file = f.name
        
        try:
            collector = DataCollector()
            collector.read_csv_file(temp_file)
            # Не должно быть ошибки, но данные не должны добавиться
            assert len(collector.name) == 0
        finally:
            os.unlink(temp_file)
    
    def test_read_csv_file_returns_tabulate(self, sample_csv_file):
        """Тест возвращаемого значения метода read_csv_file"""
        collector = DataCollector()
        result = collector.read_csv_file(sample_csv_file)
        
        assert result is not None
        assert isinstance(result, str)
        assert 'students' in result
        assert 'price' in result
        assert 'Алексей Смирнов' in result
        assert 'Дарья Петрова' in result


class TestBaseReport:
    """Тесты для абстрактного класса BaseReport"""
    
    @pytest.fixture
    def sample_csv_file(self):
        data = [
            ['student', 'date', 'coffee_spent', 'sleep_hours'],
            ['Алексей Смирнов', '2024-06-01', '450', '4.5'],
            ['Дарья Петрова', '2024-06-01', '200', '7.0'],
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(data)
            temp_file = f.name
        
        yield temp_file
        os.unlink(temp_file)
    
    def test_base_report_cannot_be_instantiated(self):
        """Тест: абстрактный класс нельзя инстанциировать"""
        with pytest.raises(TypeError):
            BaseReport()
    
    def test_concrete_class_must_implement_abstract_methods(self):
        """Тест: конкретный класс должен реализовать абстрактные методы"""
        class IncompleteReport(BaseReport):
            pass
        
        with pytest.raises(TypeError):
            IncompleteReport()
    
    def test_read_csv_files(self, sample_csv_file):
        """Тест чтения CSV файлов"""
        class ConcreteReport(BaseReport):
            def generate(self):
                return []
            
            def get_headers(self):
                return []
            
            def get_tablefmt(self):
                return 'grid'
        
        report = ConcreteReport()
        report.read_csv_files([sample_csv_file])
        
        assert len(report.data) == 2
        assert 'Алексей Смирнов' in report.data
        assert 'Дарья Петрова' in report.data
        assert report.data['Алексей Смирнов'] == [450]
        assert report.data['Дарья Петрова'] == [200]
    
    def test_read_multiple_csv_files(self, sample_csv_file):
        """Тест чтения нескольких CSV файлов"""
        class ConcreteReport(BaseReport):
            def generate(self):
                return []
            
            def get_headers(self):
                return []
            
            def get_tablefmt(self):
                return 'grid'
        
        report = ConcreteReport()
        report.read_csv_files([sample_csv_file, sample_csv_file])
        
        assert len(report.data['Алексей Смирнов']) == 2
        assert report.data['Алексей Смирнов'] == [450, 450]
    
    def test_is_valid_row(self):
        """Тест валидации строки"""
        class ConcreteReport(BaseReport):
            def generate(self):
                return []
            
            def get_headers(self):
                return []
            
            def get_tablefmt(self):
                return 'grid'
        
        report = ConcreteReport()
        
        # Валидная строка
        valid_row = {'student': 'Test', 'coffee_spent': '100'}
        assert report._is_valid_row(valid_row) is True
        
        # Пустое имя студента
        empty_name_row = {'student': '', 'coffee_spent': '100'}
        assert report._is_valid_row(empty_name_row) is False
        
        # Отсутствует student
        no_student_row = {'coffee_spent': '100'}
        assert report._is_valid_row(no_student_row) is False
        
        # Пустой coffee_spent
        empty_coffee_row = {'student': 'Test', 'coffee_spent': ''}
        assert report._is_valid_row(empty_coffee_row) is False
        
        # Отсутствует coffee_spent
        no_coffee_row = {'student': 'Test'}
        assert report._is_valid_row(no_coffee_row) is False
    
    def test_process_row_valid(self):
        """Тест обработки валидной строки"""
        class ConcreteReport(BaseReport):
            def generate(self):
                return []
            
            def get_headers(self):
                return []
            
            def get_tablefmt(self):
                return 'grid'
        
        report = ConcreteReport()
        row = {'student': 'Test', 'coffee_spent': '100'}
        report._process_row(row)
        
        assert 'Test' in report.data
        assert report.data['Test'] == [100]
    
    def test_process_row_invalid_coffee(self):
        """Тест обработки строки с некорректным coffee_spent"""
        class ConcreteReport(BaseReport):
            def generate(self):
                return []
            
            def get_headers(self):
                return []
            
            def get_tablefmt(self):
                return 'grid'
        
        report = ConcreteReport()
        row = {'student': 'Test', 'coffee_spent': 'invalid'}
        report._process_row(row)
        
        # Студент не должен быть добавлен
        assert 'Test' not in report.data


class TestMedianCoffeeReport:
    """Тесты для MedianCoffeeReport"""
    
    def test_generate_with_data(self):
        """Тест генерации отчета с данными"""
        report = MedianCoffeeReport()
        report.data['Студент 1'] = [100, 200, 300]
        report.data['Студент 2'] = [400, 500]
        report.data['Студент 3'] = [1000]  # Одно значение
        
        result = report.generate()
        
        # Проверяем количество записей
        assert len(result) == 3
        
        # Проверяем медианы
        result_dict = dict(result)
        assert result_dict['Студент 1'] == 200
        assert result_dict['Студент 2'] == 450  # (400+500)/2 = 450
        assert result_dict['Студент 3'] == 1000
        
        # Проверяем сортировку по убыванию
        medians = [price for _, price in result]
        assert medians == sorted(medians, reverse=True)
    
    def test_generate_with_empty_data(self):
        """Тест генерации отчета с пустыми данными"""
        report = MedianCoffeeReport()
        result = report.generate()
        
        assert result == []
    
    def test_generate_with_single_student(self):
        """Тест генерации отчета с одним студентом"""
        report = MedianCoffeeReport()
        report.data['Студент'] = [10, 20, 30, 40]
        
        result = report.generate()
        assert len(result) == 1
        assert result[0][1] == 25  # Медиана четного количества
    
    def test_generate_ignores_empty_prices(self):
        """Тест: студенты без данных игнорируются"""
        report = MedianCoffeeReport()
        report.data['Студент 1'] = []  # Пустой список
        report.data['Студент 2'] = [100, 200]
        
        result = report.generate()
        assert len(result) == 1
        assert result[0][0] == 'Студент 2'
    
    def test_get_headers(self):
        """Тест получения заголовков"""
        report = MedianCoffeeReport()
        headers = report.get_headers()
        assert headers == ['Student', 'Median Coffee Spent']
    
    def test_get_tablefmt(self):
        """Тест получения формата таблицы"""
        report = MedianCoffeeReport()
        assert report.get_tablefmt() == 'grid'


class TestReportFactory:
    """Тесты для фабрики отчетов"""
    
    def test_create_median_coffee_report(self):
        """Тест создания отчета median-coffee"""
        report = ReportFactory.create_report('median-coffee')
        assert isinstance(report, MedianCoffeeReport)
    
    def test_create_unknown_report(self):
        """Тест создания неизвестного отчета"""
        with pytest.raises(ValueError) as exc_info:
            ReportFactory.create_report('unknown-report')
        assert 'Unknown report type: unknown-report' in str(exc_info.value)
    
    def test_register_new_report(self):
        """Тест регистрации нового отчета"""
        class NewReport(BaseReport):
            def generate(self):
                return []
            
            def get_headers(self):
                return []
            
            def get_tablefmt(self):
                return 'grid'
        
        ReportFactory.register_report('new-report', NewReport)
        report = ReportFactory.create_report('new-report')
        assert isinstance(report, NewReport)
    
    def test_register_overwrite_report(self):
        """Тест перезаписи существующего отчета"""
        class CustomReport(BaseReport):
            def generate(self):
                return []
            
            def get_headers(self):
                return []
            
            def get_tablefmt(self):
                return 'grid'
        
        ReportFactory.register_report('median-coffee', CustomReport)
        report = ReportFactory.create_report('median-coffee')
        assert isinstance(report, CustomReport)


class TestParseArguments:
    """Тесты для парсинга аргументов"""
    
    def test_parse_arguments_with_files_and_report(self):
        """Тест парсинга с файлами и отчетом"""
        with patch('sys.argv', ['script.py', '-f', 'file1.csv', 'file2.csv', '-r', 'median-coffee']):
            args = parse_arguments()
            assert args.files == ['file1.csv', 'file2.csv']
            assert args.report == 'median-coffee'
    
    def test_parse_arguments_with_single_file(self):
        """Тест парсинга с одним файлом"""
        with patch('sys.argv', ['script.py', '-f', 'file1.csv', '-r', 'median-coffee']):
            args = parse_arguments()
            assert args.files == ['file1.csv']
            assert args.report == 'median-coffee'
    
    def test_parse_arguments_missing_files(self):
        """Тест: отсутствие обязательного параметра --files"""
        with patch('sys.argv', ['script.py', '-r', 'median-coffee']):
            with pytest.raises(SystemExit):
                parse_arguments()
    
    def test_parse_arguments_missing_report(self):
        """Тест: отсутствие обязательного параметра --report"""
        with patch('sys.argv', ['script.py', '-f', 'file1.csv']):
            with pytest.raises(SystemExit):
                parse_arguments()


class TestMainFunction:
    """Тесты для основной функции main"""
    
    @pytest.fixture
    def sample_csv_file(self):
        data = [
            ['student', 'date', 'coffee_spent', 'sleep_hours', 'study_hours', 'mood', 'exam'],
            ['Алексей Смирнов', '2024-06-01', '450', '4.5', '12', 'норм', 'Математика'],
            ['Алексей Смирнов', '2024-06-02', '500', '4.0', '14', 'устал', 'Математика'],
            ['Дарья Петрова', '2024-06-01', '200', '7.0', '6', 'отл', 'Математика'],
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(data)
            temp_file = f.name
        
        yield temp_file
        os.unlink(temp_file)
    
    @pytest.fixture
    def multiple_csv_files(self):
        data1 = [
            ['student', 'date', 'coffee_spent', 'sleep_hours', 'study_hours', 'mood', 'exam'],
            ['Алексей Смирнов', '2024-06-01', '450', '4.5', '12', 'норм', 'Математика'],
        ]
        
        data2 = [
            ['student', 'date', 'coffee_spent', 'sleep_hours', 'study_hours', 'mood', 'exam'],
            ['Алексей Смирнов', '2024-06-02', '500', '4.0', '14', 'устал', 'Математика'],
            ['Дарья Петрова', '2024-06-01', '200', '7.0', '6', 'отл', 'Математика'],
        ]
        
        files = []
        for data in [data1, data2]:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(data)
                files.append(f.name)
        
        yield files
        
        for file in files:
            os.unlink(file)
    
    def test_main_with_valid_data(self, sample_csv_file, capsys):
        """Тест main с валидными данными"""
        with patch('sys.argv', ['script.py', '-f', sample_csv_file, '-r', 'median-coffee']):
            main()
            
            captured = capsys.readouterr()
            assert 'students' in captured.out
            assert 'price' in captured.out
            assert 'Алексей Смирнов' in captured.out
            assert 'Дарья Петрова' in captured.out
    
    def test_main_with_multiple_files(self, multiple_csv_files, capsys):
        """Тест main с несколькими файлами"""
        with patch('sys.argv', ['script.py', '-f'] + multiple_csv_files + ['-r', 'median-coffee']):
            main()
            
            captured = capsys.readouterr()
            # Проверяем, что данные из обоих файлов объединены
            assert 'Алексей Смирнов' in captured.out
            assert 'Дарья Петрова' in captured.out
    
    def test_main_with_empty_data(self, capsys):
        """Тест main с пустыми данными"""
        empty_data = [
            ['student', 'date', 'coffee_spent'],
            ['', '2024-06-01', ''],
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(empty_data)
            temp_file = f.name
        
        try:
            with patch('sys.argv', ['script.py', '-f', temp_file, '-r', 'median-coffee']):
                main()
                
                captured = capsys.readouterr()
                # Должна быть пустая таблица или только заголовки
                assert 'students' in captured.out
        finally:
            os.unlink(temp_file)
    
    def test_main_with_unknown_report(self, sample_csv_file, capsys):
        """Тест main с неизвестным типом отчета"""
        with patch('sys.argv', ['script.py', '-f', sample_csv_file, '-r', 'unknown-report']):
            # В текущей реализации main не обрабатывает исключения
            # Это приведет к ошибке, но мы ее ожидаем
            with pytest.raises(ValueError):
                main()
    
    def test_main_with_file_not_found(self, capsys):
        """Тест main с несуществующим файлом"""
        with patch('sys.argv', ['script.py', '-f', 'nonexistent.csv', '-r', 'median-coffee']):
            with pytest.raises(FileNotFoundError):
                main()


class TestIntegration:
    """Интеграционные тесты"""
    
    @pytest.fixture
    def complex_csv_files(self):
        """Создание сложных CSV файлов для интеграционного тестирования"""
        data1 = [
            ['student', 'date', 'coffee_spent', 'sleep_hours', 'study_hours', 'mood', 'exam'],
            ['Алексей Смирнов', '2024-06-01', '450', '4.5', '12', 'норм', 'Математика'],
            ['Алексей Смирнов', '2024-06-02', '500', '4.0', '14', 'устал', 'Математика'],
            ['Иван Кузнецов', '2024-06-01', '600', '3.0', '15', 'зомби', 'Математика'],
        ]
        
        data2 = [
            ['student', 'date', 'coffee_spent', 'sleep_hours', 'study_hours', 'mood', 'exam'],
            ['Алексей Смирнов', '2024-06-03', '550', '3.5', '16', 'зомби', 'Математика'],
            ['Дарья Петрова', '2024-06-01', '200', '7.0', '6', 'отл', 'Математика'],
            ['Дарья Петрова', '2024-06-02', '250', '6.5', '8', 'норм', 'Математика'],
            ['Иван Кузнецов', '2024-06-02', '650', '2.5', '17', 'зомби', 'Математика'],
        ]
        
        files = []
        for data in [data1, data2]:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(data)
                files.append(f.name)
        
        yield files
        
        for file in files:
            os.unlink(file)
    
    def test_full_workflow_with_factory(self, complex_csv_files):
        """Тест полного workflow с фабрикой"""
        # Создаем отчет через фабрику
        report = ReportFactory.create_report('median-coffee')
        
        # Читаем файлы
        report.read_csv_files(complex_csv_files)
        
        # Генерируем данные
        result = report.generate()
        
        # Проверяем результаты
        assert len(result) == 3
        
        result_dict = {name: price for name, price in result}
        
        # Алексей: [450, 500, 550] -> медиана 500
        assert result_dict['Алексей Смирнов'] == 500
        
        # Дарья: [200, 250] -> медиана 225
        assert result_dict['Дарья Петрова'] == 225
        
        # Иван: [600, 650] -> медиана 625
        assert result_dict['Иван Кузнецов'] == 625
        
        # Проверяем сортировку
        medians = [price for _, price in result]
        assert medians == [625, 500, 225]
    
    def test_data_aggregation_from_multiple_files(self, complex_csv_files):
        """Тест агрегации данных из нескольких файлов"""
        report = MedianCoffeeReport()
        report.read_csv_files(complex_csv_files)
        
        # Проверяем, что данные правильно накопились
        assert len(report.data['Алексей Смирнов']) == 3
        assert len(report.data['Дарья Петрова']) == 2
        assert len(report.data['Иван Кузнецов']) == 2
        
        # Проверяем корректность медиан
        assert median(report.data['Алексей Смирнов']) == 500
        assert median(report.data['Дарья Петрова']) == 225
        assert median(report.data['Иван Кузнецов']) == 625


# Конфигурация для pytest
@pytest.fixture(autouse=True)
def cleanup_report_factory():
    """Очистка фабрики отчетов после каждого теста"""
    yield
    # Восстанавливаем исходное состояние фабрики
    ReportFactory._reports = {
        'median-coffee': MedianCoffeeReport,
    }