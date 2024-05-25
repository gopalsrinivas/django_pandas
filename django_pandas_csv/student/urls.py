from django.urls import path
from .views import StudentImportView

urlpatterns = [
    path('import/', StudentImportView.as_view(), name='import'),
]
