"""Parse SUC MSD."""

import typing as t
from enum import auto

import attrs
from strenum import StrEnum


class SucPos(StrEnum):
    """Part of Speech tags for SUC."""

    AB = auto()
    DT = auto()
    HA = auto()
    HD = auto()
    HP = auto()
    HS = auto()
    IE = auto()
    IN = auto()
    JJ = auto()
    KN = auto()
    NN = auto()
    PC = auto()
    PL = auto()
    PM = auto()
    PN = auto()
    PP = auto()
    PS = auto()
    RG = auto()
    RO = auto()
    SN = auto()
    UO = auto()
    VB = auto()


class SucDelimiter(StrEnum):
    """Delimiter in SUC."""

    MID = auto()
    MAD = auto()
    PAD = auto()


class SucComparation(StrEnum):
    """Comparation in SUC."""

    POS = auto()
    KOM = auto()
    SUV = auto()


class SucGenus(StrEnum):
    """Genus in SUC."""

    UTR = auto()
    NEU = auto()
    MAS = auto()
    UTR_NEU = "UTR/NEU"


class SucNumerus(StrEnum):
    """Numerus in SUC."""

    SIN = auto()
    PLU = auto()
    SIN_PLU = "SIN/PLU"


class SucDefinite(StrEnum):
    """Definite in SUC."""

    IND = auto()
    DEF = auto()
    IND_DEF = "IND/DEF"


class SucNounForm(StrEnum):
    """Noun form in SUC."""

    NOM = auto()
    GEN = auto()
    SMS = auto()


class SucVerbForm(StrEnum):
    """Verb form in SUC."""

    PRS = auto()
    PRT = auto()
    INF = auto()
    SUP = auto()
    IMP = auto()
    AKT = auto()
    SFO = auto()
    KON = auto()
    PRF = auto()


class SucPartOfSentence(StrEnum):
    """Part of Sentence in SUC."""

    SUB = auto()
    OBJ = auto()
    SUB_OBJ = "SUB/OBJ"


@attrs.define
class Msd:
    """MSD in SUC."""


@attrs.define
class MsdWDelimiter(Msd):
    """MSD with delimiter."""

    delimiter: SucDelimiter


@attrs.define
class MsdWPos(Msd):
    """MSD with POS."""

    pos: SucPos
    comparation: t.Optional[SucComparation] = None
    genus: t.Optional[SucGenus] = None
    numerus: t.Optional[SucNumerus] = None
    definite: t.Optional[SucDefinite] = None
    noun_form: t.Optional[SucNounForm] = None
    is_abbreviation: bool = False
    verb_forms: t.Optional[list[SucVerbForm]] = None
    part_of_sentence: t.Optional[SucPartOfSentence] = None

    @property
    def verb_form(self) -> t.Optional[SucVerbForm]:
        """Get first verb form if present."""
        return self.verb_forms[0] if self.verb_forms else None

    @classmethod
    def pos_ab(cls, pos: SucPos, comparation: t.Optional[SucComparation]) -> "MsdWPos":
        """Create MSD with POS='AB'."""
        _check_given_pos(pos, SucPos.AB)
        return cls(pos=pos, comparation=comparation)

    @classmethod
    def pos_abbr(cls, pos: SucPos) -> "MsdWPos":
        """Create MSD as abbreviation."""
        return cls(pos=pos, is_abbreviation=True)

    @classmethod
    def pos_dt(
        cls,
        pos: SucPos,
        genus: t.Optional[SucGenus],
        numerus: t.Optional[SucNumerus],
        definite: t.Optional[SucDefinite],
    ) -> "MsdWPos":
        """Create MSD with POS='DT'."""
        _check_given_pos(pos, SucPos.DT)
        return cls(pos=pos, genus=genus, numerus=numerus, definite=definite)

    @classmethod
    def pos_hd(
        cls,
        pos: SucPos,
        genus: t.Optional[SucGenus],
        numerus: t.Optional[SucNumerus],
        definite: t.Optional[SucDefinite],
    ) -> "MsdWPos":
        """Create MSD with POS='HD'."""
        _check_given_pos(pos, SucPos.HD)
        return cls(pos=pos, genus=genus, numerus=numerus, definite=definite)

    @classmethod
    def pos_hp(
        cls,
        pos: SucPos,
        genus: t.Optional[SucGenus],
        numerus: t.Optional[SucNumerus],
        definite: t.Optional[SucDefinite],
    ) -> "MsdWPos":
        """Create MSD with POS='HP'."""
        _check_given_pos(pos, SucPos.HP)
        return cls(pos=pos, genus=genus, numerus=numerus, definite=definite)

    @classmethod
    def pos_hs(
        cls,
        pos: SucPos,
        definite: t.Optional[SucDefinite],
    ) -> "MsdWPos":
        """Create MSD with POS='HS'."""
        _check_given_pos(pos, SucPos.HS)
        return cls(pos=pos, definite=definite)

    @classmethod
    def pos_jj(
        cls,
        pos: SucPos,
        comparation: SucComparation,
        genus: t.Optional[SucGenus],
        numerus: t.Optional[SucNumerus],
        definite: t.Optional[SucDefinite],
        noun_form: t.Optional[SucNounForm],
    ) -> "MsdWPos":
        """Create MSD with POS='JJ'."""
        _check_given_pos(pos, SucPos.JJ)
        return cls(
            pos=pos, comparation=comparation, genus=genus, numerus=numerus, definite=definite, noun_form=noun_form
        )

    @classmethod
    def pos_nn(
        cls,
        pos: SucPos,
        genus: t.Optional[SucGenus],
        numerus: t.Optional[SucNumerus],
        definite: t.Optional[SucDefinite],
        noun_form: t.Optional[SucNounForm],
    ) -> "MsdWPos":
        """Create MSD with POS='NN'."""
        _check_given_pos(pos, SucPos.NN)
        return cls(pos=pos, genus=genus, numerus=numerus, definite=definite, noun_form=noun_form)

    @classmethod
    def pos_pc(
        cls,
        pos: SucPos,
        verb_form: SucVerbForm,
        genus: t.Optional[SucGenus],
        numerus: t.Optional[SucNumerus],
        definite: t.Optional[SucDefinite],
        noun_form: t.Optional[SucNounForm],
    ) -> "MsdWPos":
        """Create MSD with POS='PC'."""
        _check_given_pos(pos, SucPos.PC)
        return cls(
            pos=pos, verb_forms=[verb_form], genus=genus, numerus=numerus, definite=definite, noun_form=noun_form
        )

    @classmethod
    def pos_pm(
        cls,
        pos: SucPos,
        noun_form: SucNounForm,
    ) -> "MsdWPos":
        """Create MSD with POS='PM'."""
        _check_given_pos(pos, SucPos.PM)
        return cls(pos=pos, noun_form=noun_form)

    @classmethod
    def pos_pn(
        cls,
        pos: SucPos,
        genus: t.Optional[SucGenus],
        numerus: t.Optional[SucNumerus],
        definite: t.Optional[SucDefinite],
        part_of_sentence: SucPartOfSentence,
    ) -> "MsdWPos":
        """Create MSD with POS='PN'."""
        _check_given_pos(pos, SucPos.PN)
        return cls(pos=pos, genus=genus, numerus=numerus, definite=definite, part_of_sentence=part_of_sentence)

    @classmethod
    def pos_ps(
        cls,
        pos: SucPos,
        genus: t.Optional[SucGenus],
        numerus: t.Optional[SucNumerus],
        definite: t.Optional[SucDefinite],
    ) -> "MsdWPos":
        """Create MSD with POS='PS'."""
        _check_given_pos(pos, SucPos.PS)
        return cls(pos=pos, genus=genus, numerus=numerus, definite=definite)

    @classmethod
    def pos_rg(
        cls,
        pos: SucPos,
        noun_form: SucNounForm,
    ) -> "MsdWPos":
        """Create MSD with POS='RG'."""
        _check_given_pos(pos, SucPos.RG)
        return cls(pos=pos, noun_form=noun_form)

    @classmethod
    def pos_ro(
        cls,
        pos: SucPos,
        genus: t.Optional[SucGenus],
        numerus: t.Optional[SucNumerus],
        definite: t.Optional[SucDefinite],
        noun_form: t.Optional[SucNounForm],
    ) -> "MsdWPos":
        """Create MSD with POS='RO'."""
        _check_given_pos(pos, SucPos.RO)
        return cls(pos=pos, genus=genus, numerus=numerus, definite=definite, noun_form=noun_form)

    @classmethod
    def pos_vb(
        cls,
        pos: SucPos,
        verb_forms: list[SucVerbForm],
    ) -> "MsdWPos":
        """Create MSD with POS='VB'."""
        _check_given_pos(pos, SucPos.VB)
        return cls(pos=pos, verb_forms=verb_forms)

    @classmethod
    def with_pos(cls, pos: SucPos) -> "MsdWPos":
        """Create MSD with given pos."""
        return cls(pos=pos)


def _check_given_pos(pos: SucPos, expected_pos: SucPos) -> None:
    if pos != expected_pos:
        raise ValueError(f"Got pos='{pos}', expected '{expected_pos}'")


def parse(msd: str) -> Msd:
    """Parse a str as a SUC MSD."""
    parts = msd.split(".")
    tail = parts[1:]
    try:
        pos = SucPos(parts[0])
        return _parse_from_pos(pos, tail)
    except ValueError:
        pass
    try:
        delimiter = SucDelimiter(parts[0])
        return _parse_from_delimiter(delimiter, tail)
    except ValueError:
        pass
    raise NotImplementedError(f"Unknown {msd=}")


def _parse_from_pos(pos: SucPos, msds: list[str]) -> Msd:
    if msds and msds[0] == "AN":
        return MsdWPos.pos_abbr(pos=pos)
    if pos == SucPos.AB:
        comparation = SucComparation(msds[0]) if msds else None
        return MsdWPos.pos_ab(pos=pos, comparation=comparation)
    if pos == SucPos.DT:
        genus = SucGenus(msds[0].replace("+", "/"))
        numerus = SucNumerus(msds[1].replace("+", "/"))
        definite = SucDefinite(msds[2].replace("+", "/"))
        return MsdWPos.pos_dt(pos=pos, genus=genus, numerus=numerus, definite=definite)
    if pos == SucPos.HD:
        genus = SucGenus(msds[0].replace("+", "/"))
        numerus = SucNumerus(msds[1].replace("+", "/"))
        definite = SucDefinite(msds[2].replace("+", "/"))
        return MsdWPos.pos_hd(pos=pos, genus=genus, numerus=numerus, definite=definite)
    if pos == SucPos.HP:
        genus = SucGenus(msds[0].replace("+", "/")) if msds[0] != "-" else None
        numerus = SucNumerus(msds[1].replace("+", "/")) if msds[1] != "-" else None
        definite = SucDefinite(msds[2].replace("+", "/")) if msds[2] != "-" else None
        return MsdWPos.pos_hp(pos=pos, genus=genus, numerus=numerus, definite=definite)
    if pos == SucPos.HS:
        definite = SucDefinite(msds[0].replace("+", "/"))
        return MsdWPos.pos_hs(pos=pos, definite=definite)
    if pos == SucPos.JJ:
        comparation = SucComparation(msds[0])
        genus = SucGenus(msds[1].replace("+", "/"))
        numerus = SucNumerus(msds[2].replace("+", "/"))
        definite = SucDefinite(msds[3].replace("+", "/"))
        noun_form = SucNounForm(msds[4])
        return MsdWPos.pos_jj(
            pos=pos, comparation=comparation, genus=genus, numerus=numerus, definite=definite, noun_form=noun_form
        )
    if pos == SucPos.NN:
        try:
            genus = SucGenus(msds[0].replace("+", "/")) if msds[0] != "-" else None
            numerus = SucNumerus(msds[1].replace("+", "/")) if msds[1] != "-" else None
            definite = SucDefinite(msds[2].replace("+", "/")) if msds[2] != "-" else None
            noun_form = SucNounForm(msds[3]) if msds[3] != "-" else None
        except ValueError as exc:
            raise UnsupportedValueError from exc
        return MsdWPos.pos_nn(pos=pos, genus=genus, numerus=numerus, definite=definite, noun_form=noun_form)
    if pos == SucPos.PC:
        try:
            verb_form = SucVerbForm(msds[0])
            genus = SucGenus(msds[1].replace("+", "/")) if msds[1] != "-" else None
            numerus = SucNumerus(msds[2].replace("+", "/")) if msds[2] != "-" else None
            definite = SucDefinite(msds[3].replace("+", "/")) if msds[3] != "-" else None
            noun_form = SucNounForm(msds[4]) if msds[3] != "-" else None
        except ValueError as exc:
            raise UnsupportedValueError from exc
        return MsdWPos.pos_pc(
            pos=pos, verb_form=verb_form, genus=genus, numerus=numerus, definite=definite, noun_form=noun_form
        )
    if pos == SucPos.PM:
        noun_form = SucNounForm(msds[0])
        return MsdWPos.pos_pm(pos=pos, noun_form=noun_form)
    if pos == SucPos.PN:
        try:
            genus = SucGenus(msds[0].replace("+", "/")) if msds[0] != "-" else None
            numerus = SucNumerus(msds[1].replace("+", "/")) if msds[1] != "-" else None
            definite = SucDefinite(msds[2].replace("+", "/")) if msds[2] != "-" else None
            part_of_sentence = SucPartOfSentence(msds[3].replace("+", "/"))
        except ValueError as exc:
            raise UnsupportedValueError from exc
        return MsdWPos.pos_pn(
            pos=pos, genus=genus, numerus=numerus, definite=definite, part_of_sentence=part_of_sentence
        )
    if pos == SucPos.PS:
        genus = SucGenus(msds[0].replace("+", "/"))
        numerus = SucNumerus(msds[1].replace("+", "/"))
        definite = SucDefinite(msds[2].replace("+", "/"))
        return MsdWPos.pos_ps(pos=pos, genus=genus, numerus=numerus, definite=definite)
    if pos == SucPos.RG:
        noun_form = SucNounForm(msds[0])
        return MsdWPos.pos_rg(pos=pos, noun_form=noun_form)
    if pos == SucPos.RO:
        try:
            genus = SucGenus(msds[0].replace("+", "/")) if msds[0] != "-" else None
            numerus = SucNumerus(msds[1].replace("+", "/")) if msds[1] != "-" else None
            definite = SucDefinite(msds[2].replace("+", "/")) if msds[2] != "-" else None
            noun_form = SucNounForm(msds[3]) if msds[3] != "-" else None
        except ValueError as exc:
            try:
                noun_form = SucNounForm(msds[0])
                return MsdWPos.pos_ro(pos=pos, genus=None, numerus=None, definite=None, noun_form=noun_form)
            except ValueError as exc:
                raise UnsupportedValueError from exc
            raise UnsupportedValueError from exc
        return MsdWPos.pos_ro(pos=pos, genus=genus, numerus=numerus, definite=definite, noun_form=noun_form)

    if pos == SucPos.VB:
        try:
            verb_forms = [SucVerbForm(msds[0])]
            verb_forms.append(SucVerbForm(msds[1]))
            if verb_forms[0] == SucVerbForm.KON:
                verb_forms.append(SucVerbForm(msds[2]))
        except ValueError as exc:
            raise UnsupportedValueError from exc
        return MsdWPos.pos_vb(pos=pos, verb_forms=verb_forms)
    return MsdWPos.with_pos(pos=pos)


class UnsupportedValueError(Exception):
    """Unsupported value for a MSD."""


def _parse_from_delimiter(delimiter: SucDelimiter, _msds: list[str]) -> Msd:
    return MsdWDelimiter(delimiter=delimiter)
