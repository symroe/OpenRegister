from collections import namedtuple, defaultdict

import requests

import requests_cache
requests_cache.install_cache('register_cache')


CACHE = {}


class BaseRegisterObject():

    @classmethod
    def create(cls, name, phase, *args, **kwargs):
        cache_key = "--".join([cls.meta_type, phase, name])

        if cache_key in CACHE:
            return CACHE[cache_key]

        instance = cls()
        instance.name = name
        instance.phase = phase
        if hasattr(instance, '_create_extra'):
            instance._create_extra(*args, **kwargs)
        instance._build_meda_data()
        CACHE[cache_key] = instance
        return instance


    def __repr__(self):
        return "<{}: {}>".format(
            self.meta_type,
            self.name,
            )

    @property
    def meta_data_url(self):
        return "http://{}.{}.openregister.org/record/{}.json".format(
            self.meta_type,
            self.phase,
            self.name,
        )

    def _get_json(self, url):
        # print("{}: {}".format(self.meta_type, url))
        return requests.get(url).json()


class Register(BaseRegisterObject):
    meta_type = 'register'

    additional_fields = [
        'entry_timestamp',
        'entry_number',
        'item_hash',
    ]

    def _create_extra(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._records = {}

    @property
    def url(self):
        return "https://{}.{}.openregister.org/".format(
            self.name,
            self.phase,
        )

    def _build_meda_data(self):

        self.meta_data = self._get_json(self.meta_data_url)
        field_objects = []

        for field_name in self.meta_data['fields']:
            field_objects.append(Field.create(field_name, self.phase))

        for field_name in self.additional_fields:
            field_objects.append(Field.create(field_name, self.phase, virtual=True))
        self.meta_data['fields'] = field_objects

    def _format_field_name(self, f):
        return f.replace('-', '_')

    def _get_record_tuple(self):
        fields = [
            self._format_field_name(f.name) for f in self.meta_data['fields']]
        return namedtuple('Record', fields)

    @property
    def records(self):
        if self._records:
            return self._records

        url = "{}records.json?page-size=5000".format(self.url)
        all_records = self._get_json(url)
        record_tuple = self._get_record_tuple()

        for record, data in all_records.items():
            record_data = {}

            cleaned_data = {
                self._format_field_name(k): v
                for k, v in data.items()
            }

            for field in self.meta_data['fields']:
                cleaned_field_name = self._format_field_name(field.name)
                if cleaned_field_name in cleaned_data:
                    record_data[cleaned_field_name] = FieldValue(
                        cleaned_data[cleaned_field_name],
                        field)
                else:
                    record_data[cleaned_field_name] = FieldValue("", field)

            self._records[record] = record_tuple(**record_data)
        return self._records


class Field(BaseRegisterObject):
    meta_type = 'field'

    def _create_extra(self, *args, **kwargs):
        self.virtual = kwargs.pop('virtual', False)
        super().__init__(*args, **kwargs)

    def _build_meda_data(self):
        if self.virtual:
            self.meta_data = {}
            return self.meta_data

        self.meta_data = self._get_json(self.meta_data_url)


class FieldValue(object):
    def __init__(self, value, field_class=None):
        self.raw_value = value
        self.field_class = field_class

    @property
    def value(self):
        if not self.raw_value:
            return None
        meta_data = getattr(self.field_class, 'meta_data', {})
        if 'datatype' in meta_data and meta_data.get('datatype') == 'curie':
            register_name, record_id = self.raw_value.split(':')
            r = Register.create(register_name, self.field_class.phase)
            return r.records[record_id].name.value

        return self.raw_value

    def __repr__(self):
        return "<FieldValue: {} ({})>".format(
            self.raw_value or "[Blank]",
            self.field_class.meta_data.get('datatype')
            )


def get_all_registers_with_field(fieldname, phase='alpha'):
    DEAD_REGISTERS = [
        # 'school-authority',
        # 'school-trust',
    ]

    all_registers = []
    url = "http://register.{}.openregister.org/records.json".format(
        phase
    )

    all_register_data = requests.get(url).json()

    for register_name, register_data in all_register_data.items():
        if register_name in DEAD_REGISTERS:
            continue
        if fieldname in register_data['fields']:
            all_registers.append(Register.create(register_name, phase))
    return all_registers


def check_registers_exist(phase):

    register = Register.create('register', phase)
    for register_name in register.records:
        try:
            r = Register.create(register_name, phase).records
        except requests.exceptions.ConnectionError:
            print("BROKEN: {}".format(register_name))


if __name__ == "__main__":

    field = 'organisation'

    all_registers = get_all_registers_with_field(
        field, phase='alpha')
    for register in all_registers:
        for key, record in register.records.items():
            safe_field = field.replace('-', '_')
            print(register, getattr(record, safe_field).value)


    # for phase in ['discovery', 'alpha', 'beta']:
    #     print(phase)
    #     check_registers_exist(phase)
