from padar_converter.mhealth import dataset


class Grouper:
    def __init__(self, data_inputs):
        self._data_inputs = data_inputs

    def get_group(self, grouping_func):
        return grouping_func(self._data_inputs)


class MHealthGrouper(Grouper):
    def pid_group(self):
        return self.get_group(
            lambda inputs: list(
                map(dataset.get_pid, inputs)
            )
        )

    def sid_group(self):
        return self.get_group(
            lambda inputs: list(
                map(dataset.get_sid, inputs)
            )
        )

    def annotator_group(self):
        return self.get_group(
            lambda inputs: list(
                map(dataset.get_sid, inputs)
            )
        )

    def sensor_type_group(self):
        return self.get_group(
            lambda inputs: list(
                map(dataset.get_sensor_type, inputs)
            )
        )

    def file_type_group(self):
        return self.get_group(
            lambda inputs: list(
                map(dataset.get_file_type, inputs)
            )
        )

    def data_type_group(self):
        return self.get_group(
            lambda inputs: list(
                map(dataset.get_data_type, inputs)
            )
        )
