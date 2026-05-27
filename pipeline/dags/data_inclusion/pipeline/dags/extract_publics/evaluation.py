import mlflow
import mlflow.genai
from mlflow.entities import Feedback
from mlflow.genai import datasets, scorers
from mlflow.genai.evaluation.entities import EvaluationResult

from data_inclusion.pipeline.dags.extract_publics import constants, extraction


@scorers.scorer
def assert_expected_value(outputs: dict, expectations: dict) -> Feedback:
    expected_value = expectations.get("expected_value")

    if expected_value is None:
        return Feedback(
            value=None,
            rationale="No expected value provided",
        )

    if expected_value == outputs:
        return Feedback(value=True, rationale="Output matches expected value")
    else:
        return Feedback(
            value=False,
            rationale=f"Output does not match expected value: {expected_value}",
        )


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

    with mlflow.context(tags={"workflow": constants.PROMPT_EVAL_ALIAS}):
        return mlflow.genai.evaluate(
            data=dataset.to_df(),
            predict_fn=extraction.extract,
            scorers=[
                assert_expected_value,
            ],
        )


if __name__ == "__main__":
    eval()
