from src import typedefs
from pydantic import TypeAdapter, ValidationError
from typing import Callable
import json



EXAMPLE = """
{
  "stats": {
    "1": {
      "currentTime": 1708119363909075,
      "rsos": [
        {
          "id": 0,
          "pipelineIDs": [
            0
          ],
          "stats": {
            "prunedFiles": 0,
            "scanAssignedBytes": 0,
            "scanAssignedFiles": 0,
            "scanBytes": 0,
            "scanFiles": 0
          },
          "type": 9
        },
        {
          "id": 2,
          "pipelineIDs": [
            1
          ],
          "stats": {
            "inputRows": 284353,
            "outputRows": 284353
          },
          "type": 71
        },
        {
          "id": 4,
          "pipelineIDs": [
            1
          ],
          "stats": {
            "inputRows": 284412,
            "outputRows": 284412
          },
          "type": 49
        },
        {
          "id": 6,
          "pipelineIDs": [
            2
          ],
          "stats": {
            "prunedFiles": 0,
            "scanAssignedBytes": 0,
            "scanAssignedFiles": 0,
            "scanBytes": 0,
            "scanFiles": 0
          },
          "type": 9
        },
        {
          "id": 8,
          "pipelineIDs": [
            2
          ],
          "stats": {
            "inputRows": 284334,
            "outputRows": 284334
          },
          "type": 49
        },
        {
          "id": 10,
          "pipelineIDs": [
            3
          ],
          "stats": {
            "prunedFiles": 0,
            "scanAssignedBytes": 97057395200,
            "scanAssignedFiles": 6616,
            "scanBytes": 0,
            "scanFiles": 0
          },
          "type": 9
        },
        {
          "id": 15,
          "pipelineIDs": [
            3
          ],
          "stats": {
            "inputRows0": 568495,
            "inputRows1": 52476251,
            "outputRows": 19888
          },
          "type": 51
        },
        {
          "id": 20,
          "pipelineIDs": [
            4
          ],
          "stats": {
            "inputRows": 0,
            "outputRows": 0
          },
          "type": 71
        },
        {
          "id": 28,
          "pipelineIDs": [
            3,
            4
          ],
          "stats": {
            "inputRows0": 568495,
            "inputRows1": 19888,
            "outputRows": 19888
          },
          "type": 51
        },
        {
          "id": 32,
          "pipelineIDs": [
            5
          ],
          "stats": {
            "inputRows": 19498,
            "outputRows": 0
          },
          "type": 60
        },
        {
          "id": 51,
          "pipelineIDs": [
            6
          ],
          "stats": {
            "inputRows": 0,
            "outputRows": 0
          },
          "type": 68
        },
        {
          "id": 53,
          "pipelineIDs": [
            7
          ],
          "stats": {
            "inputRows": 0,
            "outputRows": 0
          },
          "type": 71
        }
      ],
      "startTime": 1708119248664278,
      "version": 1
    },
    "2": {
      "currentTime": 1708119368826707,
      "rsos": [
        {
          "id": 0,
          "pipelineIDs": [
            0
          ],
          "stats": {
            "prunedFiles": 0,
            "scanAssignedBytes": 4680704,
            "scanAssignedFiles": 2,
            "scanBytes": 0,
            "scanFiles": 0
          },
          "type": 9
        },
        {
          "id": 2,
          "pipelineIDs": [
            1
          ],
          "stats": {
            "inputRows": 284142,
            "outputRows": 284142
          },
          "type": 71
        },
        {
          "id": 4,
          "pipelineIDs": [
            1
          ],
          "stats": {
            "inputRows": 284083,
            "outputRows": 284083
          },
          "type": 49
        },
        {
          "id": 6,
          "pipelineIDs": [
            2
          ],
          "stats": {
            "prunedFiles": 0,
            "scanAssignedBytes": 4680704,
            "scanAssignedFiles": 2,
            "scanBytes": 0,
            "scanFiles": 0
          },
          "type": 9
        },
        {
          "id": 8,
          "pipelineIDs": [
            2
          ],
          "stats": {
            "inputRows": 284161,
            "outputRows": 284161
          },
          "type": 49
        },
        {
          "id": 10,
          "pipelineIDs": [
            3
          ],
          "stats": {
            "prunedFiles": 0,
            "scanAssignedBytes": 102716924416,
            "scanAssignedFiles": 7004,
            "scanBytes": 0,
            "scanFiles": 0
          },
          "type": 9
        },
        {
          "id": 15,
          "pipelineIDs": [
            3
          ],
          "stats": {
            "inputRows0": 568495,
            "inputRows1": 45294061,
            "outputRows": 19517
          },
          "type": 51
        },
        {
          "id": 20,
          "pipelineIDs": [
            4
          ],
          "stats": {
            "inputRows": 0,
            "outputRows": 0
          },
          "type": 71
        },
        {
          "id": 28,
          "pipelineIDs": [
            3,
            4
          ],
          "stats": {
            "inputRows0": 568495,
            "inputRows1": 19517,
            "outputRows": 19517
          },
          "type": 51
        },
        {
          "id": 32,
          "pipelineIDs": [
            5
          ],
          "stats": {
            "inputRows": 19868,
            "outputRows": 0
          },
          "type": 60
        },
        {
          "id": 51,
          "pipelineIDs": [
            6
          ],
          "stats": {
            "inputRows": 0,
            "outputRows": 0
          },
          "type": 68
        },
        {
          "id": 53,
          "pipelineIDs": [
            7
          ],
          "stats": {
            "inputRows": 0,
            "outputRows": 0
          },
          "type": 71
        }
      ],
      "startTime": 1708119248667799,
      "version": 1
    }
  },
  "time": 120
}
"""

def assert_validation_error(x: Callable):
    try:
        x()
    except ValidationError:
        return
    raise Exception("Expected a validation error")

# def test_simple():
#     ta = TypeAdapter(types.RsoTiming)
#     x = ta.validate_python({'first': 100, 'second': 100})
#     n1a = TypeAdapter(types.Nest1)
#     n1 = n1a.validate_python({'a': x})
#     assert_validation_error(lambda: ta.validate_python({'a': x}))
#     assert_validation_error(lambda: n1a.validate_python({'a': "other"}))
#     n2a = TypeAdapter(types.Nest2)
#     n2 = n2a.validate_python({'a': n1})

def test_model_version():
    typedefs.RsoTiming.model_validate({'first': 1714181713.079, 'second': 1714181713.079, 'extra': 'idc'})

def test_parse_example():
    f = open('./test/testdata/example_payload.json')
    data = json.load(f)
    print(typedefs.Entry.model_validate(data))
    assert(False)

import pickle
def test_parse_pickle():
    f = open('./test/testdata/example_payload.json')
    data = json.load(f)

    model = typedefs.Entry.model_validate(data)
    pickle.dumps(model)
    pickle.dumps([model])
    assert(False)