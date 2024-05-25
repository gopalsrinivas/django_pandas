from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .models import Student
from .serializers import StudentSerializer
import pandas as pd
import os
from django.conf import settings


class StudentImportView(APIView):
    def get(self, request, *args, **kwargs):
        students = Student.objects.all()
        serializer = StudentSerializer(students, many=True)
        return Response(serializer.data)

    def post(self, request, *args, **kwargs):
        file = request.FILES.get('file')
        if not file:
            return Response({"error": "No file uploaded"}, status=status.HTTP_400_BAD_REQUEST)

        if not (file.name.endswith('.csv') or file.name.endswith('.xlsx')):
            return Response({"error": "Unsupported file format"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            if file.name.endswith('.csv'):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)

            df.fillna(value='Unknown', inplace=True)

            for _, row in df.iterrows():
                student, created = Student.objects.update_or_create(
                    name=row.get('name', 'Unknown'),
                    age=row.get('age', 'Unknown'),
                    city=row.get('city', 'Unknown'),
                )
                if not created:
                    return Response({"error": "Data already exists"}, status=status.HTTP_400_BAD_REQUEST)

            return Response({"message": "Data imported successfully"}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
