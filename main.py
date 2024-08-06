from utils import download_files

from recipes import Recipes


if __name__ == "__main__":

    file_name = "recipe.json"

    download_files(
        url="https://bnlf-tests.s3.eu-central-1.amazonaws.com/recipes.json",
        output_filename=file_name
    )

    recipes = Recipes()

    df = recipes.get_df_from_file(file_name, objects_denoted_with_comma=False)
    df = recipes.search_for_word(df, "ingredients", "chilies|chiles")
    result_df = (
        recipes.extract_cooking_dificulty_based_on_time_to_cook_in_minutes(df)
    )
    recipes.generate_results_file("recipes-etl", "Results.csv", result_df)
