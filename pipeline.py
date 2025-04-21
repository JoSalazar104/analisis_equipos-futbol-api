import os
import requests
import pandas as pd

class Pipeline:
    def __init__(self, seasons, team,outputfolder):
        #seasons: Lista de temporadas.
        #team: ID del equipo
        self.seasons = seasons
        self.api_key = '10c63ea9a623b4054c1b48a3de24e9ee'
        self.team = team
        self.data = []
        self.output_folder= outputfolder

    def extract(self):
        for season in self.seasons:
            url = f'https://v2.nba.api-sports.io/games?season={season}&team={self.team}'
            headers = {
                'x-apisports-key': self.api_key,
                'x-apisports-host': "v3.nba.api-sports.io"
            }
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                season_data = response.json()
                self.data.append(season_data)  #Se almacenan los json en una lista
            else:
                print(f'Error al obtener los datos de la temporada {season}: {response.status_code}')

    def transform(self):
        if not self.data:
            raise Exception('No hay datos para transformar. Ejecute el método extract() primero')

        dfs = []
        for season_data in self.data:
            df = pd.json_normalize(season_data['response'])
            dfs.append(df)

        """1ra Transformación"""
        df_final = pd.concat(dfs, ignore_index=True)

        """2da Transformación"""
        columnas = ['id','season', 'stage', 'date.start', 'arena.name', 'arena.city', 'arena.state', 
                    'teams.visitors.id', 'teams.visitors.name', 'teams.visitors.nickname', 'teams.visitors.code', 
                    'teams.home.id', 'teams.home.name', 'teams.home.nickname', 'teams.home.code', 
                    'scores.visitors.linescore', 'scores.visitors.points', 
                    'scores.home.linescore', 'scores.home.points']
        df_final = df_final[columnas]

        """3era Transformación"""
        df_final = df_final[df_final['stage'] != 1]

        """4rta Transformación"""
        df_final['winner'] = df_final.apply(
            lambda row: row['teams.visitors.name'] if row['scores.visitors.points'] > row['scores.home.points'] 
            else row['teams.home.name'], axis=1
        )

        """5ta Transformación"""
        df_final['date.start'] = pd.to_datetime(df_final['date.start']).dt.strftime('%Y-%m-%d %H:%M')

        """6ta Transformación"""
        df_final['diferencia_puntos'] = abs(df_final['scores.visitors.points'] - df_final['scores.home.points'])

        return df_final

    def load(self):
        if self.data is None or not isinstance(self.data, pd.DataFrame):
            raise Exception('No hay datos para cargar. Ejecute el método transform() primero.')

        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

        output_path = os.path.join(self.output_folder, f'{self.team}_in{self.seasons}_games.csv')
        print("Guardando archivo en:", output_path)  # Línea de depuración

        self.data.to_csv(output_path, index=False)  # Aquí ahora sí guardamos el DataFrame


    def run(self):
        self.extract()
        self.data=self.transform()
        self.load()