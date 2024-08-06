import polars as pl
import os

from polars.dataframe import DataFrame


class Recipes:

    def get_df_from_file(
        self, file_name: str, objects_denoted_with_comma: bool = True
    ):
        if objects_denoted_with_comma:
            return pl.read_json(file_name).unique()
        else:
            return pl.read_ndjson(file_name).unique()

    def extract_minutes(self, time_str):
        time_str = time_str.replace('PT', '')

        total_minutes = 0

        if 'H' in time_str:
            hours_str = time_str.split('H')[0]
            total_minutes += int(hours_str) * 60

        if 'M' in time_str:
            minutes_part = time_str.split('M')[0]
            if 'H' in minutes_part:
                minutes_str = minutes_part.split('H')[1]
            else:
                minutes_str = minutes_part
            total_minutes += int(minutes_str)

        return total_minutes

    def search_for_word(self, df: DataFrame, column_name: str, word: str):
        # use (?xi) to make it a case-insensitive search
        return df.filter(
            pl.col(column_name).str.contains(fr"(?xi){word}")
        )

    def extract_cooking_dificulty_based_on_time_to_cook_in_minutes(
        self, df: DataFrame
    ):

        df = df.with_columns([
            pl.col("cookTime").map_elements(
                self.extract_minutes, return_dtype=int).alias("cookMin"),
            pl.col("prepTime").map_elements(
                self.extract_minutes, return_dtype=int).alias("prepMin")
        ])

        df = df.with_columns([
            (pl.col("cookMin") + pl.col("prepMin")).alias("totalMin")
        ])

        df = df.with_columns([
            pl
            .when(pl.col("totalMin") == 0).then(pl.lit("Unknown"))
            .when(pl.col("totalMin") > 60).then(pl.lit("Hard"))
            .when(
                (pl.col("totalMin") > 30) & (pl.col("totalMin") <= 60)
            ).then(pl.lit("Medium"))
            .otherwise(pl.lit("Easy"))
            .alias("difficulty")
        ])

        result_df = df.group_by("difficulty").agg(
            pl.col("totalMin").mean().round(2).alias("AverageTotalTime")
        ).sort("difficulty")

        # Ensure we only have the required difficulty
        # levels - this removes (Unknown column)
        required_difficulties = ["Easy", "Medium", "Hard"]
        result_df = (
            result_df.filter(pl.col("difficulty").is_in(required_difficulties))
        )

        return result_df

    def generate_results_file(
        self, folder_name: str, file_name: str, result_df: DataFrame
    ):
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        file_path = os.path.join(folder_name, file_name)
        result_df.write_csv(file_path, separator="|")
