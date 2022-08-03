from typing import Any, Callable, Dict, Mapping, Optional


class InvalidParamError(ValueError):
    pass


ParamFunc = Callable[[str], Any]


class ParamConfig:
    PARAM_PROCESSORS: Mapping[str, Optional[ParamFunc]] = {}

    def __init__(
        self,
        param_processors: Optional[Mapping[str, Optional[ParamFunc]]] = None,
    ) -> None:
        self.param_processors = {**self.PARAM_PROCESSORS}
        if param_processors is not None:
            self.param_processors.update(param_processors)

    def process_params(
        self,
        params: Mapping[str, str],
        subject: str = "",
    ) -> Dict[str, Any]:
        result = {}
        for key, value in params.items():
            try:
                func = self.param_processors[key]
            except KeyError as exc:
                subject_msg = f" for {subject}" if subject else ""
                base_msg = f"Invalid parameter: {key}"
                if self.param_processors:
                    params_str = " ".join(self.param_processors.keys())
                    supported_msg = (
                        f"Supported parameters{subject_msg} are: {params_str}"
                    )
                else:
                    supported_msg = f"No parameters are supported{subject_msg}"
                raise InvalidParamError(
                    f"{base_msg}\n{supported_msg}",
                ) from exc
            else:
                if func is None:
                    result[key] = value
                else:
                    result[key] = func(value)
        return result
