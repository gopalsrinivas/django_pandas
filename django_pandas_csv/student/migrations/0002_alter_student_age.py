# Generated by Django 5.0.4 on 2024-05-25 11:50

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("student", "0001_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="student",
            name="age",
            field=models.IntegerField(null=True),
        ),
    ]
