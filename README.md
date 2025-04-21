# 🏀 Análisis de Partidos de la NBA usando API Sports

Este proyecto conecta con una API deportiva para extraer, transformar y analizar información de partidos de la NBA por temporada y equipo. Incluye un pipeline ETL en Python que permite automatizar la recolección, limpieza y exportación de los datos en formato CSV para análisis posterior.

## 🚀 ¿Qué hace este proyecto?

- Se conecta a la API de `v2.nba.api-sports.io` usando una clave personal.
- Extrae partidos por ID de equipo y temporada.
- Transforma el JSON en un DataFrame consolidado, filtrado y enriquecido.
- Calcula campos como el equipo ganador, diferencia de puntos y fecha en formato legible.
- Exporta los datos procesados a un archivo `.csv`.

## 🧪 Tecnologías usadas

- Python 3.x
- `requests` – conexión a la API
- `pandas` – transformación de datos
- `os` – gestión de carpetas y archivos

## 📁 Estructura del proyecto

nba-api-analysis/ 

├── notebook.ipynb 

├── pipeline.py #Clase Pipeline con métodos extract(), transform(), load() 

├── README.md 

