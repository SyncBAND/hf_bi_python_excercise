import polars as pl
import unittest

from unittest.mock import patch, mock_open

from recipes import Recipes


class TestRecipe(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.recipes = Recipes()

    @patch('main.download_files')
    @patch.object(Recipes, 'get_df_from_file', new_callable=mock_open)
    def test_get_df_from_file(self, mock_get_df_from_file, mock_download_file):
        # Mocking the download file method
        mock_download_file.return_value = None

        mock_get_df_from_file.return_value = pl.DataFrame(
            {"name": ["Easter Sandwich", "Pasta with Pesto Cream"]}
        )

        # Now call get_df_from_file
        df = self.recipes.get_df_from_file("recipe.json")

        # Check the DataFrame returned
        self.assertEqual(df.shape, (2, 1))
        self.assertEqual(df.columns, ["name"])
        self.assertEqual(
            df["name"].to_list(), ["Easter Sandwich", "Pasta with Pesto Cream"]
        )

        # Ensure open was called with the correct filename and mode
        mock_get_df_from_file.assert_called_once_with("recipe.json")

    def test_extract_minutes(self):
        self.assertEqual(self.recipes.extract_minutes(''), 0)
        self.assertEqual(self.recipes.extract_minutes('PT'), 0)
        self.assertEqual(self.recipes.extract_minutes('PT45M'), 45)
        self.assertEqual(self.recipes.extract_minutes('PT1H30M'), 90)
        self.assertEqual(self.recipes.extract_minutes('PT2H'), 120)

    def test_search_for_word(self):
        data = {
            "name": ["Easter Sandwich", "Pasta with Pesto Cream"],
            "ingredients": ["chilies, Grated Cheese", "chiles, Chicken"]
        }
        df = pl.DataFrame(data)

        # test with chilies
        result_df = self.recipes.search_for_word(df, "ingredients", "chilies")
        self.assertEqual(result_df.shape, (1, 2))
        self.assertEqual(result_df[0, "name"], "Easter Sandwich")

        # test for chilies and chiles
        result_df = self.recipes.search_for_word(
            df, "ingredients", "chilies|chiles"
        )
        self.assertEqual(result_df.shape, (2, 2))
        self.assertEqual(
            df["name"].to_list(), ["Easter Sandwich", "Pasta with Pesto Cream"]
        )

    def test_determine_dificulty_based_on_time_to_cook_in_minutes(self):
        data = {
            "name": ["Easter Sandwich", "Pasta with Pesto Cream", "Recipe3"],
            "cookTime": ["PT30M", "PT1H", "PT15M"],
            "prepTime": ["PT15M", "PT30M", "PT15M"]
        }
        df = pl.DataFrame(data)
        result_df = (
            self.recipes
            .determine_dificulty_based_on_time_to_cook_in_minutes(df)
        )

        self.assertEqual(result_df.shape, (3, 7))
        self.assertIn("Medium", result_df["difficulty"].to_list())
        self.assertIn("Hard", result_df["difficulty"].to_list())
        self.assertIn("Easy", result_df["difficulty"].to_list())

        self.assertNotEqual(result_df[0, "difficulty"], "Hard")
        self.assertEqual(result_df[0, "difficulty"], "Medium")
        self.assertEqual(result_df[1, "difficulty"], "Hard")
        self.assertEqual(result_df[2, "difficulty"], "Easy")

    def test_get_average_based_on_difficulty(self):
        data = {
            "name": ["Easter Sandwich", "Pasta with Pesto Cream", "Recipe3"],
            "cookTime": ["PT30M", "PT1H", "PT15M"],
            "prepTime": ["PT15M", "PT30M", "PT15M"],
            "cookMin": [30, 60, 15],
            "prepMin": [15, 30, 15],
            "totalMin": [45, 90, 30],
            "difficulty": ['Medium', 'Hard', 'Easy']
        }
        df = pl.DataFrame(data)
        result_df = self.recipes.get_average_based_on_difficulty(df)

        self.assertEqual(result_df.shape, (3, 3))
        self.assertIn("difficulty", result_df.columns)
        self.assertIn("AverageTotalTime", result_df.columns)
        self.assertIn("SumOfTotalTime", result_df.columns)

    @patch('os.makedirs')
    @patch('polars.DataFrame.write_csv')
    def test_generate_csv_file(self, mock_write_csv, mock_makedirs):
        result_df = pl.DataFrame({
            "difficulty": ["Easy", "Medium", "Hard"],
            "AverageTotalTime": [30, 45, 75],
            "SumOfTotalTime": [120, 200, 350]
        })

        self.recipes.generate_csv_file(
            "recipes-etl", "Results.csv", result_df, "|"
        )
        mock_makedirs.assert_called_with("recipes-etl")
        mock_write_csv.assert_called_with(
            "recipes-etl/Results.csv", separator="|"
        )


if __name__ == "__main__":
    unittest.main()
