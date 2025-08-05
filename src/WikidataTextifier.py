
from .WikidataLabel import LazyLabelFactory
from .utils import get_wikidata_entities_by_ids, get_lang_val
from datetime import datetime, date
from dataclasses import dataclass
import re


@dataclass
class WikidataText:
    text: str | None = None

    @classmethod
    def from_raw(cls, value, lazylabel):
        if isinstance(value, str):
            return cls(
                text=value,
            )

        if isinstance(value, dict):
            if 'id' in value:
                return cls(
                    text=value.get('id'),
                )
            if 'language' in value:
                if value.get('language') == lazylabel.lang:
                    return cls(
                        text=value.get('text', ''),
                    )
                else:
                    return cls(
                        text=None,
                    )

        return cls(
            text=str(value),
        )

    def __str__(self):
        return str(self.text) if self.text else ""

    def to_json(self):
        if self.text is None:
            return None

        return self.text

@dataclass
class WikidataCoordinates:
    latitude: float | None = None
    longitude: float | None = None

    @classmethod
    def from_raw(cls, value, lazylabel):
        if not isinstance(value, dict):
            return cls(
                time=None,
                precision=None,
                calendarmodel=None
            )

        return cls(
            latitude=value.get('latitude'),
            longitude=value.get('longitude')
        )

    def __str__(self):
        latitude = abs(self.latitude)
        hemi = 'N' if self.latitude >= 0 else 'S'

        degrees = int(latitude)
        minutes_full = (latitude - degrees) * 60
        minutes = int(minutes_full)
        seconds = (minutes_full - minutes) * 60

        # Round to-tenth of a second, drop trailing .0
        seconds = round(seconds, 1)
        seconds_str = f"{seconds}".rstrip("0").rstrip(".")

        lat_str = f"{degrees}°{minutes}'{seconds_str}\"{hemi}"

        longitude = abs(self.longitude)
        hemi = 'E' if self.longitude >= 0 else 'W'

        degrees = int(longitude)
        minutes_full = (longitude - degrees) * 60
        minutes = int(minutes_full)
        seconds = (minutes_full - minutes) * 60

        # Round to-tenth of a second, drop trailing .0
        seconds = round(seconds, 1)
        seconds_str = f"{seconds}".rstrip("0").rstrip(".")

        lon_str = f"{degrees}°{minutes}'{seconds_str}\"{hemi}"

        return f'{lat_str}, {lon_str}'

    def to_json(self):
        return {
            'latitude': self.latitude,
            'longitude': self.longitude
        }

@dataclass
class WikidataTime:
    time: str | None = None
    precision: int | None = None
    calendarmodel: str | None = None

    @classmethod
    def from_raw(cls, value, lazylabel):
        if not isinstance(value, dict):
            return cls(
                time=None,
                precision=None,
                calendarmodel=None
            )

        calendarmodel = value.get('calendarmodel', 'Q1985786')
        calendarmodel = calendarmodel.split('/')[-1]
        return cls(
            time=value.get('time'),
            precision=value.get('precision'),
            calendarmodel=calendarmodel
        )

    def __str__(self):
        # Use regex to parse the time string
        pattern = r'([+-])(\d{1,16})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z'
        match = re.match(pattern, self.time)

        if not match:
            raise ValueError("Malformed time string")

        sign, year_str, month_str, day_str, hour_str, minute_str, second_str = match.groups()
        year = int(year_str) * (1 if sign == '+' else -1)

        # Convert Julian to Gregorian if necessary
        if 'Q1985786' in self.calendarmodel and year > 1 and len(str(abs(year))) <= 4:  # Julian calendar
            try:
                month = 1 if month_str == '00' else int(month_str)
                day = 1 if day_str == '00' else int(day_str)
                julian_date = date(year, month, day)
                gregorian_ordinal = julian_date.toordinal() + (datetime(1582, 10, 15).toordinal() - datetime(1582, 10, 5).toordinal())
                gregorian_date = date.fromordinal(gregorian_ordinal)
                year, month, day = gregorian_date.year, gregorian_date.month, gregorian_date.day
            except ValueError:
                raise ValueError("Invalid date for Julian calendar")
        else:
            month = int(month_str) if month_str != '00' else 1
            day = int(day_str) if day_str != '00' else 1

        # Next step: take translations from Wikidata Labels
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        month_str = months[month - 1] if month != 0 else ''
        era = 'AD' if year > 0 else 'BC'

        if self.precision == 14:
            return f"{year} {month_str} {day} {hour_str}:{minute_str}:{second_str}"
        elif self.precision == 13:
            return f"{year} {month_str} {day} {hour_str}:{minute_str}"
        elif self.precision == 12:
            return f"{year} {month_str} {day} {hour_str}:00"
        elif self.precision == 11:
            return f"{day} {month_str} {year}"
        elif self.precision == 10:
            return f"{month_str} {year}"
        elif self.precision == 9:
            return f"{abs(year)} {era}"
        elif self.precision == 8:
            decade = (year // 10) * 10
            return f"{abs(decade)}s {era}"
        elif self.precision == 7:
            century = (abs(year) - 1) // 100 + 1
            return f"{century}th century {era}"
        elif self.precision == 6:
            millennium = (abs(year) - 1) // 1000 + 1
            return f"{millennium}th millennium {era}"
        elif self.precision == 5:
            tens_of_thousands = abs(year) // 10000
            return f"{tens_of_thousands} ten thousand years {era}"
        elif self.precision == 4:
            hundreds_of_thousands = abs(year) // 100000
            return f"{hundreds_of_thousands} hundred thousand years {era}"
        elif self.precision == 3:
            millions = abs(year) // 1000000
            return f"{millions} million years {era}"
        elif self.precision == 2:
            tens_of_millions = abs(year) // 10000000
            return f"{tens_of_millions} tens of millions of years {era}"
        elif self.precision == 1:
            hundreds_of_millions = abs(year) // 100000000
            return f"{hundreds_of_millions} hundred million years {era}"
        elif self.precision == 0:
            billions = abs(year) // 1000000000
            return f"{billions} billion years {era}"
        else:
            raise ValueError(f"Unknown precision value {self.precision}")

    def to_json(self):
        return {
            'time': self.time,
            'precision': self.precision,
            'calendar_QID': self.calendarmodel
        }

@dataclass
class WikidataQuantity:
    amount: str | None
    unit: str | None = None
    unit_id: str | None = None

    @classmethod
    def from_raw(cls, value, lazylabel):
        if not isinstance(value, dict):
            return cls(
                amount=None,
                unit=None
            )

        unit = value.get('unit')
        amount = value.get('amount')
        if unit == '1':
            unit = None
            unit_id = None
        else:
            unit_id = unit.rsplit('/')[-1]
            unit = lazylabel.create(unit_id)

        return cls(
            amount=amount,
            unit=unit,
            unit_id=unit_id
        )

    def __str__(self):
        return f"{self.amount} {self.unit or ''}".strip()

    def to_json(self):
        return {
            'amount': self.amount,
            'unit': self.unit,
            'unit_QID': self.unit_id
        }


@dataclass
class WikidataClaimValue:
    claim: "WikidataClaim"
    value: "WikidataEntity | WikidataQuantity | WikidataTime | WikidataCoordinates | WikidataText | None"
    qualifiers: list["WikidataClaim"]

    @classmethod
    def from_raw(cls, claim, value, qualifiers, lazylabel):
        if value.get('value') is None:
            return cls(
                claim=claim,
                value=None,
                qualifiers=[]
            )
        elif value.get('type') == 'wikibase-entityid':
            parsed_value = WikidataEntity(
                id=value['value']['id'],
                label=lazylabel.create(value['value']['id']),
                description=None,
                aliases=[],
                instanceof=[],
                claims=[]
            )
        elif value.get('type') == 'quantity':
            parsed_value = WikidataQuantity.from_raw(
                value['value'], lazylabel
            )
        elif value.get('type') == 'time':
            parsed_value = WikidataTime.from_raw(
                value['value'], lazylabel
            )
        elif value.get('type') == 'globecoordinate':
            parsed_value = WikidataCoordinates.from_raw(
                value['value'], lazylabel
            )
        else:
            parsed_value = WikidataText.from_raw(
                value['value'], lazylabel
            )

        # Setup the qualifiers
        parsed_qualifiers = []
        for pid, qualifier in qualifiers.items():
            parsed_qualifiers.append(
                WikidataClaim.from_raw(
                    subject=None,
                    property=WikidataEntity(
                        id=pid,
                        label=lazylabel.create(pid),
                        description=None,
                        aliases=[],
                        instanceof=[],
                        claims=[]
                    ),
                    claim=qualifier,
                    lazylabel=lazylabel
                )
            )

        return cls(
            claim=claim,
            value=parsed_value,
            qualifiers=parsed_qualifiers
        )

    def __str__(self):
        if not self:
            return ''

        string = str(self.value)
        attributes = [str(q) for q in self.qualifiers if q]
        if len(attributes) > 0:
            string += f" ({', '.join(attributes)})"
        return string

    def __bool__(self):
        return (self.value is not None) and str(self.value) != ''

    def to_json(self):
        if not self:
            return None

        value = self.value.to_json()
        if isinstance(self.value, WikidataEntity):
            ID_name = "QID" if self.claim.datatype == 'wikibase-item' else "PID"
            value = {
                ID_name: value['QID'],
                'label': value['label']
            }

        qualifiers = [q.to_json() for q in self.qualifiers if q]
        if len(qualifiers) == 0:
            return {
                "value": value
            }

        return {
            "value": value,
            "qualifiers": [q.to_json() for q in self.qualifiers if q]
        }


@dataclass
class WikidataClaim:
    subject: "WikidataEntity"
    property: "WikidataEntity"
    values: list["WikidataClaimValue"]
    datatype: str

    @classmethod
    def from_raw(cls, subject, property, claim, lazylabel, external_ids=True):
        if not claim:
            return cls(
                subject=subject,
                property=property,
                values=[],
                datatype='empty'
            )

        datatype = claim[0].get('mainsnak', claim[0])\
                            .get('datatype', {})
        if not external_ids and datatype == 'external-id':
            return cls(
                subject=subject,
                property=property,
                values=[],
                datatype=datatype
            )

        rank_preferred_found = False
        for i in range(len(claim)):
            claim[i]['datavalue'] = claim[i].get('mainsnak', claim[i])\
                                            .get('datavalue', {})
            rank_preferred_found = rank_preferred_found or \
                                (claim[i].get('rank') == 'preferred')

        # Include only rank preferred claims or rank normal if preferred is not found.
        for i in range(len(claim)):
            if 'rank' not in claim[i]:
                claim[i]['rank'] = 'normal'

            is_rank_normal = (claim[i].get('rank') == 'normal')
            is_rank_preferred = (claim[i].get('rank') == 'preferred')
            rank_normal_condition = is_rank_normal and \
                                    (not rank_preferred_found)
            rank_preferred_condition = is_rank_preferred and \
                                    rank_preferred_found
            claim[i]['include'] = rank_normal_condition or \
                                  rank_preferred_condition

        values = [
            WikidataClaimValue.from_raw(
                claim=None,
                value=value.get('datavalue', {}),
                qualifiers=value.get('qualifiers', {}),
                lazylabel=lazylabel
            ) for value in claim if value['include']
        ]

        claim_obj = cls(
            subject=subject,
            property=property,
            values=values,
            datatype=datatype
        )

        # circular backreference
        for val in values:
            val.claim = claim_obj
        return claim_obj


    def __str__(self):
        if not self:
            return ''

        if not str(self.property.label):
            return ''

        values = [str(v) for v in self.values if v]
        values = ", ".join(values)

        return f"{str(self.property.label)}: {values}"

    def __bool__(self):
        return (self.property is not None) and \
                str(self.property) != '' and \
                    (len(self.values) > 0) and \
                        any(bool(val) for val in self.values)

    def to_json(self):
        property = self.property.to_json()
        return {
            "PID": property['QID'],
            "property_label": property['label'],
            "datatype": self.datatype,
            "values": [v.to_json() for v in self.values if v]
        }


@dataclass
class WikidataEntity:
    id: str
    label: str | None
    description: str | None
    aliases: list[str]
    instanceof: list["WikidataEntity"]
    claims: list[WikidataClaim]

    @classmethod
    def from_id(cls, id: str, lang: str = 'en', external_ids: bool = True):
        entity_dict = get_wikidata_entities_by_ids(id)
        if id not in entity_dict:
            raise ValueError(f"ID not found.")

        entity_dict = entity_dict[id]
        if 'labels' not in entity_dict:
            return None

        label = get_lang_val(entity_dict['labels'], lang)
        description = get_lang_val(entity_dict['descriptions'], lang)

        aliases = entity_dict['aliases'].get(lang, []) + \
                        entity_dict['aliases'].get('mul', [])
        aliases = list(set([alias.get('value') for alias in aliases]))

        lazylabel = LazyLabelFactory(lang=lang)

        claims = [
            WikidataClaim.from_raw(
                subject=None,
                property=WikidataEntity(
                    id=pid,
                    label=lazylabel.create(pid),
                    description=None,
                    aliases=[],
                    instanceof=[],
                    claims=[]
                ),
                claim=claim,
                lazylabel=lazylabel,
                external_ids=external_ids
            ) for pid, claim in entity_dict.get('claims', {}).items()
        ]

        instanceofclaim = [c for c in claims if c.property.id == 'P31']
        if len(instanceofclaim) > 0:
            instanceof = [val.value for val in instanceofclaim[0].values]

        entity = cls(
            id=id,
            label=label,
            description=description,
            aliases=aliases,
            instanceof=instanceof,
            claims=claims
        )

        # circular backreference
        for c in entity.claims:
            c.subject = entity
        return entity

    def __str__(self):
        label_str = str(self.label)
        string = label_str

        if self.instanceof:
            string += f" ({', '.join(map(str, self.instanceof))})"
        if self.description:
            string += f", {self.description}"
        if self.aliases:
            string += f", also known as {', '.join(map(str, self.aliases))}"

        attributes = [str(c) for c in self.claims if c]
        if len(attributes) > 0:
            attributes = "\n- ".join(attributes)
            string += f". Attributes:\n- {attributes}"
        elif string != label_str:
            string += "."

        return string

    def __bool__(self):
        return (self.id is not None) and \
               (self.label is not None) and \
               (str(self.label) != '')

    def to_json(self):
        return {
            'QID': self.id,
            'label': str(self.label),
            'description': self.description,
            'aliases': self.aliases,
            'claims': [c.to_json() for c in self.claims if c]
        }
