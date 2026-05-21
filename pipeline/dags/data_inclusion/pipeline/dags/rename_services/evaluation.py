import mlflow
import mlflow.genai
from mlflow.entities import Feedback
from mlflow.genai import datasets, scorers
from mlflow.genai.evaluation.entities import EvaluationResult

from data_inclusion.pipeline.dags.rename_services import constants, renommage


@scorers.scorer
def keyword_presence(outputs: str, expectations: dict) -> Feedback:
    expected_keyword = expectations.get("expected_keyword")

    if expected_keyword is None:
        return Feedback(
            value=None,
            rationale="No expected keywords were provided in the expectations",
        )

    if expected_keyword.lower() in outputs.lower():
        return Feedback(value=True, rationale="Keyword present")
    else:
        return Feedback(value=False, rationale=f"Missing keyword: {expected_keyword}")


def eval() -> EvaluationResult:
    dataset = datasets.get_dataset(name=constants.DATASET_NAME)

    traces_list = [
        trace
        for trace in mlflow.search_traces(
            filter_string=f"tags.workflow = '{constants.PROMPT_RUN_ALIAS}'",
            max_results=100,
            return_type="list",
        )
        if len(trace.search_assessments(type="expectation")) > 0
    ]

    dataset.merge_records(records=traces_list)

    return mlflow.genai.evaluate(
        data=dataset.to_df(),
        predict_fn=lambda service: renommage.rename(
            service=service,
            prompt_uri=constants.PROMPT_EVAL_URI,
        ),
        scorers=[keyword_presence],
    )


if __name__ == "__main__":
    eval()
