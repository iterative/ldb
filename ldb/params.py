from typing import Any, Callable, Dict, Mapping, Optional


class InvalidParamError(ValueError):
    pass


ParamFunc = Callable[[str], Any]


class ParamConfig:
    PARAM_PROCESSORS: Mapping[str, Optional[ParamFunc]] = {}

    def process_params(
        self,
        params: Mapping[str, str],
        subject: str = "",
    ) -> Dict[str, Any]:
        result = {}
        for key, value in params.items():
            try:
                func = self.PARAM_PROCESSORS[key]
            except KeyError as exc:
                subject_msg = f" for {subject}" if subject else ""
                base_msg = f"Invalid parameter: {key}"
                if self.PARAM_PROCESSORS:
                    params_str = " ".join(self.PARAM_PROCESSORS.keys())
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
