import pandas as pd
import os
from django.conf import settings
from django.core.management.base import BaseCommand
from student.models import Student


class Command(BaseCommand):
    help = 'Import students from a CSV or XLSX file'

    def add_arguments(self, parser):
        parser.add_argument('file_path', nargs='?', default='',help='Path to the CSV or XLSX file')

    def handle(self, *args, **kwargs):
        file_path = kwargs['file_path']

        if not file_path:
            data_dir = os.path.join(settings.BASE_DIR, 'data')
            file_path = os.path.join(data_dir, 'sample_csv.csv')

        if not os.path.exists(file_path):
            self.stdout.write(self.style.ERROR(f'File not found at {file_path}.'))
            return

        try:
            _, file_extension = os.path.splitext(file_path)
            chunk_size = 3  # Adjust the chunk size as needed
            imported_students = set()
            new_records = set()
            updated_records = set()
            duplicates = []
            total_records = 0

            if file_extension.lower() == '.csv':
                chunk_iterator = pd.read_csv(file_path, chunksize=chunk_size)
                for chunk_number, chunk in enumerate(chunk_iterator):
                    total_records += len(chunk)
                    self.stdout.write(self.style.SUCCESS(f'Processing chunk {chunk_number + 1}...'))
                    self.process_chunk(chunk, imported_students, new_records, updated_records, duplicates)
            elif file_extension.lower() == '.xlsx':
                xls = pd.ExcelFile(file_path)
                for sheet_number, sheet_name in enumerate(xls.sheet_names):
                    df = pd.read_excel(xls, sheet_name=sheet_name)
                    total_records += len(df)
                    for chunk_number in range(0, df.shape[0], chunk_size):
                        chunk = df.iloc[chunk_number:chunk_number + chunk_size]
                        self.stdout.write(self.style.SUCCESS(f'Processing chunk {chunk_number + 1} of sheet {sheet_number + 1}...'))
                        self.process_chunk(chunk, imported_students, new_records, updated_records, duplicates)
            else:
                self.stdout.write(self.style.ERROR('Unsupported file format. Supported formats are CSV and XLSX.'))
                return

            existing_students = set(
                Student.objects.values_list('id', flat=True))
            self.delete_obsolete_records(imported_students, existing_students)

            num_new = len(new_records)
            num_updated = len(updated_records)
            num_duplicates = len(duplicates)
            num_deleted = len(existing_students - imported_students)
            self.stdout.write(self.style.SUCCESS(f'Excel file contains {total_records} records.'))
            self.stdout.write(self.style.SUCCESS(f'{num_new} records inserted into the database.'))
            self.stdout.write(self.style.SUCCESS(f'{num_updated} records updated in the database.'))
            self.stdout.write(self.style.WARNING(f'{num_duplicates} duplicate records found.'))
            self.stdout.write(self.style.SUCCESS(f'{num_deleted} obsolete records deleted from the database.'))

            if num_duplicates > 0:
                self.stdout.write(self.style.WARNING('Duplicate records:'))
                for record in duplicates:
                    self.stdout.write(self.style.WARNING(f'{record}'))

            self.stdout.write(self.style.SUCCESS('Successfully imported students.'))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error occurred while loading the file: {str(e)}'))
            return

    def process_chunk(self, chunk, imported_students, new_records, updated_records, duplicates):
        chunk['name'] = chunk['name'].fillna(value='Unknown').str.strip()
        chunk['age'] = chunk['age'].fillna(value=0)
        chunk['city'] = chunk['city'].fillna(value='Unknown').str.strip()

        for index, row in chunk.iterrows():
            name = row['name'] if pd.notnull(row['name']) else 'Unknown'
            age = row['age'] if pd.notnull(row['age']) else 0
            city = row['city'] if pd.notnull(row['city']) else 'Unknown'

            try:
                student, created = Student.objects.update_or_create(
                    name=name,
                    age=age,
                    city=city,
                    defaults={'age': age, 'city': city}
                )
            except Student.MultipleObjectsReturned:
                self.stdout.write(self.style.ERROR(f'Multiple students found with name: {name}, age: {age}, city: {city}'))
                continue

            if created:
                new_records.add(student.id)
            else:
                if student.age != age or student.city != city:
                    student.age = age
                    student.city = city
                    student.save()
                    updated_records.add(student.id)
                else:
                    duplicates.append((name, age, city))

            imported_students.add(student.id)

    def delete_obsolete_records(self, imported_students, existing_students):
        students_to_delete = existing_students - imported_students
        Student.objects.filter(id__in=students_to_delete).delete()
