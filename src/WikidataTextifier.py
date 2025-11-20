
from .WikidataLabel import WikidataLabel, LazyLabelFactory
from .utils import get_wikidata_entities_by_ids, wikidata_time_to_text, wikidata_geolocation_to_text
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
    string_val: str | None = None

    @classmethod
    def from_raw(cls, value, lazylabel):
        if not isinstance(value, dict):
            return cls(
                latitude=None,
                longitude=None,
                string_val=None
            )

        string_val = wikidata_geolocation_to_text(
            value.get('latitude'),
            value.get('longitude')
        )

        return cls(
            latitude=value.get('latitude'),
            longitude=value.get('longitude'),
            string_val=string_val
        )

    def __str__(self):
        return self.string_val or ''

    def to_json(self):
        return {
            'latitude': self.latitude,
            'longitude': self.longitude,
            'string': self.string_val
        }

@dataclass
class WikidataTime:
    time: str | None = None
    precision: int | None = None
    calendarmodel: str | None = None
    string_val: str | None = None

    @classmethod
    def from_raw(cls, value, lazylabel):
        if not isinstance(value, dict):
            return cls(
                time=None,
                precision=None,
                calendarmodel=None,
                string_val=None
            )

        calendarmodel = value.get('calendarmodel', 'Q1985786')
        calendarmodel = calendarmodel.split('/')[-1]

        string_val = wikidata_time_to_text(value, lazylabel.lang)

        return cls(
            time=value.get('time'),
            precision=value.get('precision'),
            calendarmodel=calendarmodel,
            string_val=string_val
        )

    def __str__(self):
        return self.string_val or ''

    def to_json(self):
        return {
            'time': self.time,
            'precision': self.precision,
            'calendar_QID': self.calendarmodel,
            'string': self.string_val
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
        if self.unit_id:
            return f"{self.amount} {str(self.unit)}"
        return self.amount

    def to_json(self):
        if self.unit_id:
            return {
                'amount': self.amount,
                'unit': str(self.unit),
                'unit_QID': self.unit_id
            }
        return self.amount


@dataclass
class WikidataClaimValue:
    claim: "WikidataClaim"
    value: "WikidataEntity | WikidataQuantity | WikidataTime | WikidataCoordinates | WikidataText | None"
    qualifiers: list["WikidataClaim"] | None
    references: list[list["WikidataClaim"]] | None
    rank: str | None # 'preferred', 'normal', 'deprecated'

    @classmethod
    def from_raw(cls, claim, value, qualifiers, references, rank, lazylabel):
        if value.get('value') is None:
            return cls(
                claim=claim,
                value=None,
                qualifiers=None,
                references=None,
                rank=rank
            )
        elif value.get('type') == 'wikibase-entityid':
            id = value['value']['id']
            if id.startswith('P') or id.startswith('Q'):
                parsed_value = WikidataEntity(
                    id=id,
                    label=lazylabel.create(id),
                    description=None,
                    aliases=[],
                    claims=[]
                )
            else:
                parsed_value = WikidataText.from_raw(
                    id, lazylabel
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
                        claims=[]
                    ),
                    claim=qualifier,
                    lazylabel=lazylabel
                )
            )

        # Setup the references
        parsed_references = []
        for reference in references:
            reference_claims = reference.get('snaks', {})
            parsed_references_sublist = []
            for pid, reference_claim in reference_claims.items():
                parsed_references_sublist.append(
                    WikidataClaim.from_raw(
                        subject=None,
                        property=WikidataEntity(
                            id=pid,
                            label=lazylabel.create(pid),
                            description=None,
                            aliases=[],
                            claims=[]
                        ),
                        claim=reference_claim,
                        lazylabel=lazylabel
                    )
                )
            parsed_references.append(parsed_references_sublist)

        return cls(
            claim=claim,
            value=parsed_value,
            qualifiers=parsed_qualifiers,
            references=parsed_references,
            rank=rank
        )

    def __str__(self):
        if not self:
            return ''

        string = str(self.value)
        qualifiers = [str(q) for q in self.qualifiers if q]

        if self.rank == 'deprecated':
            string += " [deprecated]"

        if len(qualifiers) > 0:
            string += f" ({', '.join(qualifiers)})"

        return string

    def __bool__(self):
        return (self.value is not None) and str(self.value) != ''

    def to_json(self):
        if not self:
            return None

        value = self.value.to_json()
        if isinstance(self.value, WikidataEntity):
            ID_name = "QID" if self.claim.datatype == 'wikibase-item' else "PID"
            entity_id = value.get('QID') or value.get('PID')
            value = {
                ID_name: entity_id,
                'label': str(value['label'])
            }

        return_dict = {
            "value": value
        }

        if self.qualifiers:
            qualifiers = [q.to_json() for q in self.qualifiers if q]
            return_dict["qualifiers"] = qualifiers

        if self.references:
            references = [[r.to_json() for r in ref if r] \
                        for ref in self.references]
            return_dict["references"] = references

        if self.rank:
            return_dict["rank"] = self.rank

        return return_dict

    def to_triplet(self):
        if not self:
            return ''

        string = str(self.value)
        if isinstance(self.value, WikidataEntity):
            string = f"{str(self.value.label)} ({self.value.id})"

        if self.rank == 'deprecated':
            string += " [deprecated]"

        qualifiers = [q.to_triplet(as_qualifier=True) \
                      for q in self.qualifiers if q]
        if len(qualifiers) > 0:
            string += f" | {' | '.join(qualifiers)}"
        return string


@dataclass
class WikidataClaim:
    subject: "WikidataEntity"
    property: "WikidataEntity"
    values: list["WikidataClaimValue"]
    datatype: str

    @classmethod
    def from_raw(cls, subject, property, claim, lazylabel,
                 external_ids=True, references=False, all_ranks=False):
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
                # For qualifiers and references, rank is not defined
                claim[i]['rank'] = None
                claim[i]['include'] = True

            else:
                # Skip the filtering if all ranks are requested
                if all_ranks:
                    claim[i]['include'] = True
                    continue

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
                references=value.get('references', []) if references else [],
                lazylabel=lazylabel,
                rank=value.get('rank', None)
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

        values = [str(v) for v in self.values if v]
        values = ", ".join(values)

        return f"{str(self.property.label)}: {values}"

    def __bool__(self):
        return (self.property is not None) and \
                str(self.property.label) != '' and \
                    (len(self.values) > 0) and \
                        any(bool(val) for val in self.values)

    def to_json(self):
        property = self.property.to_json()
        property_id = property.get('PID') or property.get('QID')
        return {
            "PID": property_id,
            "property_label": property['label'],
            "datatype": self.datatype,
            "values": [v.to_json() for v in self.values if v]
        }

    def to_triplet(self, as_qualifier=False):
        if not self:
            return ''

        label = f"{str(self.property.label)} ({self.property.id})"
        values = [v.to_triplet() for v in self.values if v]

        if len(values) > 0:
            if as_qualifier:
                # For qualifiers: join multiple values with comma on same line
                values_str = ", ".join(values)
                return f"{label}: {values_str}"
            else:
                # For main claims: each value gets its own line
                values = [f"{label}: {v}" for v in values]
                values = "\n".join(values)
                return values

        return ''


@dataclass
class WikidataEntity:
    id: str
    label: str | None
    description: str | None
    aliases: list[str]
    claims: list[WikidataClaim]

    @classmethod
    def from_wd(cls,
                entity_dict: dict,
                id: str,
                lang: str = 'en',
                external_ids: bool = True,
                all_ranks: bool = False,
                references: bool = False,
                filter_pids: list[str] | None = None):

        if 'labels' not in entity_dict:
            return None

        label = WikidataLabel.get_lang_val(entity_dict['labels'], lang)
        description = WikidataLabel.get_lang_val(entity_dict['descriptions'], lang)

        aliases = entity_dict['aliases'].get(lang, []) + \
                        entity_dict['aliases'].get('mul', [])
        aliases = list(set([alias.get('value') \
                            if isinstance(alias, dict) \
                                else alias \
                                    for alias in aliases]))

        lazylabel = LazyLabelFactory(lang=lang)

        claims = entity_dict.get('claims', {})
        if filter_pids:
            claims = {pid: claim for pid, claim in claims.items() \
                      if pid in filter_pids}

        claims = [
            WikidataClaim.from_raw(
                subject=None,
                property=WikidataEntity(
                    id=pid,
                    label=lazylabel.create(pid),
                    description=None,
                    aliases=[],
                    claims=[]
                ),
                claim=claim,
                lazylabel=lazylabel,
                external_ids=external_ids,
                references=references,
                all_ranks=all_ranks
            ) for pid, claim in claims.items()
        ]

        entity = cls(
            id=id,
            label=label,
            description=description,
            aliases=aliases,
            claims=claims
        )

        # circular backreference
        for c in entity.claims:
            c.subject = entity
        return entity

    def __str__(self):
        label_str = str(self.label) if self.label else '<missing>'
        string = label_str

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
        id_key = 'PID' if self.id.startswith('P') else 'QID'

        return {
            id_key: self.id,
            'label': str(self.label) if self.label else None,
            'description': self.description,
            'aliases': self.aliases,
            'claims': [c.to_json() for c in self.claims if c]
        }

    def to_triplet(self):
        label = f"{str(self.label) if self.label else '<missing>'} ({self.id})"
        attributes = []
        if self.description:
            attributes.append(f"description: {self.description}")
        if self.aliases:
            attributes.append(f"aliases: {', '.join(map(str, self.aliases))}")
        attributes = [*attributes, *[c.to_triplet() for c in self.claims if c]]

        if len(attributes) > 0:
            attributes = "\n".join(attributes).split("\n")
            attributes = [f"{label}: {a}" for a in attributes]
            attributes = "\n".join(attributes)
            return attributes

        return label
