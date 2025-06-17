"""Parse SUC MSD."""

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


class SucDegree(StrEnum):
    """Degree in SUC."""

    POS = auto()
    KOM = auto()
    SUV = auto()


class SucGender(StrEnum):
    """Gender in SUC."""

    UTR = auto()
    NEU = auto()
    MAS = auto()
    UTR_NEU = "UTR/NEU"


class SucNumber(StrEnum):
    """Number in SUC."""

    SIN = auto()
    PLU = auto()
    SIN_PLU = "SIN/PLU"


class SucDefiniteness(StrEnum):
    """Definiteness in SUC."""

    IND = auto()
    DEF = auto()
    IND_DEF = "IND/DEF"


class SucCase(StrEnum):
    """Case in SUC."""

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


class SucPronounForm(StrEnum):
    """Pronoun form in SUC."""

    SUB = auto()
    OBJ = auto()
    SUB_OBJ = "SUB/OBJ"


class SucVoice(StrEnum):
    """Voice in SUC."""

    AKT = auto()
    SFO = auto()


class SucTense(StrEnum):
    """Tense in SUC."""

    IMP = auto()
    INF = auto()
    PRS = auto()
    PRT = auto()
    SUP = auto()


class SucMood(StrEnum):
    """Mood in SUC."""

    KON = auto()


class SucParticleForm(StrEnum):
    """Particle form in SUC."""

    PRS = auto()
    PRF = auto()


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
    degree: SucDegree | None = None
    gender: SucGender | None = None
    number: SucNumber | None = None
    definiteness: SucDefiniteness | None = None
    case: SucCase | None = None
    tense: SucTense | None = None
    voice: SucVoice | None = None
    mood: SucMood | None = None
    particle_form: SucParticleForm | None = None
    pronoun_form: SucPronounForm | None = None
    is_abbreviation: bool = False

    @classmethod
    def pos_ab(cls, pos: SucPos, degree: SucDegree | None) -> "MsdWPos":
        """Create MSD with POS='AB'."""
        _check_given_pos(pos, SucPos.AB)
        return cls(pos=pos, degree=degree)

    @classmethod
    def pos_abbr(cls, pos: SucPos) -> "MsdWPos":
        """Create MSD as abbreviation."""
        return cls(pos=pos, is_abbreviation=True)

    @classmethod
    def pos_dt(
        cls,
        pos: SucPos,
        gender: SucGender,
        number: SucNumber,
        definiteness: SucDefiniteness,
    ) -> "MsdWPos":
        """Create MSD with POS='DT'."""
        _check_given_pos(pos, SucPos.DT)
        return cls(pos=pos, gender=gender, number=number, definiteness=definiteness)

    @classmethod
    def pos_hd(
        cls,
        pos: SucPos,
        gender: SucGender,
        number: SucNumber,
        definiteness: SucDefiniteness | None,
    ) -> "MsdWPos":
        """Create MSD with POS='HD'."""
        _check_given_pos(pos, SucPos.HD)
        return cls(pos=pos, gender=gender, number=number, definiteness=definiteness)

    @classmethod
    def pos_hp(
        cls,
        pos: SucPos,
        gender: SucGender | None,
        number: SucNumber | None,
        definiteness: SucDefiniteness | None,
    ) -> "MsdWPos":
        """Create MSD with POS='HP'."""
        _check_given_pos(pos, SucPos.HP)
        return cls(pos=pos, gender=gender, number=number, definiteness=definiteness)

    @classmethod
    def pos_hs(
        cls,
        pos: SucPos,
        definiteness: SucDefiniteness | None,
    ) -> "MsdWPos":
        """Create MSD with POS='HS'."""
        _check_given_pos(pos, SucPos.HS)
        return cls(pos=pos, definiteness=definiteness)

    @classmethod
    def pos_jj(
        cls,
        pos: SucPos,
        degree: SucDegree,
        gender: SucGender,
        number: SucNumber,
        definiteness: SucDefiniteness,
        case: SucCase,
    ) -> "MsdWPos":
        """Create MSD with POS='JJ'."""
        _check_given_pos(pos, SucPos.JJ)
        return cls(pos=pos, degree=degree, gender=gender, number=number, definiteness=definiteness, case=case)

    @classmethod
    def pos_nn(
        cls,
        pos: SucPos,
        gender: SucGender | None,
        number: SucNumber | None,
        definiteness: SucDefiniteness | None,
        case: SucCase | None,
    ) -> "MsdWPos":
        """Create MSD with POS='NN'."""
        _check_given_pos(pos, SucPos.NN)
        return cls(pos=pos, gender=gender, number=number, definiteness=definiteness, case=case)

    @classmethod
    def pos_pc(
        cls,
        pos: SucPos,
        particle_form: SucParticleForm,
        gender: SucGender,
        number: SucNumber,
        definiteness: SucDefiniteness,
        case: SucCase,
    ) -> "MsdWPos":
        """Create MSD with POS='PC'."""
        _check_given_pos(pos, SucPos.PC)
        return cls(
            pos=pos,
            particle_form=particle_form,
            gender=gender,
            number=number,
            definiteness=definiteness,
            case=case,
        )

    @classmethod
    def pos_pm(
        cls,
        pos: SucPos,
        case: SucCase,
    ) -> "MsdWPos":
        """Create MSD with POS='PM'."""
        _check_given_pos(pos, SucPos.PM)
        return cls(pos=pos, case=case)

    @classmethod
    def pos_pn(
        cls,
        pos: SucPos,
        gender: SucGender | None,
        number: SucNumber | None,
        definiteness: SucDefiniteness | None,
        pronoun_form: SucPronounForm,
    ) -> "MsdWPos":
        """Create MSD with POS='PN'."""
        _check_given_pos(pos, SucPos.PN)
        return cls(pos=pos, gender=gender, number=number, definiteness=definiteness, pronoun_form=pronoun_form)

    @classmethod
    def pos_ps(
        cls,
        pos: SucPos,
        gender: SucGender,
        number: SucNumber,
        definiteness: SucDefiniteness | None,
    ) -> "MsdWPos":
        """Create MSD with POS='PS'."""
        _check_given_pos(pos, SucPos.PS)
        return cls(pos=pos, gender=gender, number=number, definiteness=definiteness)

    @classmethod
    def pos_rg(
        cls,
        pos: SucPos,
        case: SucCase | None,
    ) -> "MsdWPos":
        """Create MSD with POS='RG'."""
        _check_given_pos(pos, SucPos.RG)
        return cls(pos=pos, case=case)

    @classmethod
    def pos_ro(
        cls,
        pos: SucPos,
        gender: SucGender | None,
        number: SucNumber | None,
        definiteness: SucDefiniteness | None,
        case: SucCase | None,
    ) -> "MsdWPos":
        """Create MSD with POS='RO'."""
        _check_given_pos(pos, SucPos.RO)
        return cls(pos=pos, gender=gender, number=number, definiteness=definiteness, case=case)

    @classmethod
    def pos_vb(
        cls,
        pos: SucPos,
        tense: SucTense,
        mood: SucMood | None,
        voice: SucVoice,
    ) -> "MsdWPos":
        """Create MSD with POS='VB'."""
        _check_given_pos(pos, SucPos.VB)
        return cls(pos=pos, tense=tense, voice=voice, mood=mood)

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
        degree = SucDegree(msds[0]) if msds else None
        return MsdWPos.pos_ab(pos=pos, degree=degree)
    if pos == SucPos.DT:
        gender = SucGender(msds[0].replace("+", "/"))
        number = SucNumber(msds[1].replace("+", "/"))
        definiteness = SucDefiniteness(msds[2].replace("+", "/"))
        return MsdWPos.pos_dt(pos=pos, gender=gender, number=number, definiteness=definiteness)
    if pos == SucPos.HD:
        gender = SucGender(msds[0].replace("+", "/"))
        number = SucNumber(msds[1].replace("+", "/"))
        definiteness = SucDefiniteness(msds[2].replace("+", "/"))
        return MsdWPos.pos_hd(pos=pos, gender=gender, number=number, definiteness=definiteness)
    if pos == SucPos.HP:
        gender = SucGender(msds[0].replace("+", "/")) if msds[0] != "-" else None  # type: ignore[assignment]
        number = SucNumber(msds[1].replace("+", "/")) if msds[1] != "-" else None  # type: ignore[assignment]
        definiteness = SucDefiniteness(msds[2].replace("+", "/")) if msds[2] != "-" else None  # type: ignore[assignment]
        return MsdWPos.pos_hp(pos=pos, gender=gender, number=number, definiteness=definiteness)
    if pos == SucPos.HS:
        definiteness = SucDefiniteness(msds[0].replace("+", "/"))
        return MsdWPos.pos_hs(pos=pos, definiteness=definiteness)
    if pos == SucPos.JJ:
        degree = SucDegree(msds[0])
        gender = SucGender(msds[1].replace("+", "/"))
        number = SucNumber(msds[2].replace("+", "/"))
        definiteness = SucDefiniteness(msds[3].replace("+", "/"))
        case = SucCase(msds[4])
        return MsdWPos.pos_jj(
            pos=pos, degree=degree, gender=gender, number=number, definiteness=definiteness, case=case
        )
    if pos == SucPos.NN:
        try:
            gender = SucGender(msds[0].replace("+", "/")) if msds[0] != "-" else None  # type: ignore[assignment]
            number = SucNumber(msds[1].replace("+", "/")) if msds[1] != "-" else None  # type: ignore[assignment]
            definiteness = SucDefiniteness(msds[2].replace("+", "/")) if msds[2] != "-" else None  # type: ignore[assignment]
            case = SucCase(msds[3]) if msds[3] != "-" else None  # type: ignore[assignment]
        except ValueError as exc:
            raise UnsupportedValueError from exc
        return MsdWPos.pos_nn(pos=pos, gender=gender, number=number, definiteness=definiteness, case=case)
    if pos == SucPos.PC:
        try:
            particle_form = SucParticleForm(msds[0])
            gender = SucGender(msds[1].replace("+", "/"))
            number = SucNumber(msds[2].replace("+", "/"))
            definiteness = SucDefiniteness(msds[3].replace("+", "/"))
            case = SucCase(msds[4])
        except ValueError as exc:
            raise UnsupportedValueError from exc
        return MsdWPos.pos_pc(
            pos=pos, particle_form=particle_form, gender=gender, number=number, definiteness=definiteness, case=case
        )
    if pos == SucPos.PM:
        case = SucCase(msds[0])
        return MsdWPos.pos_pm(pos=pos, case=case)
    if pos == SucPos.PN:
        try:
            gender = SucGender(msds[0].replace("+", "/")) if msds[0] != "-" else None  # type: ignore[assignment]
            number = SucNumber(msds[1].replace("+", "/")) if msds[1] != "-" else None  # type: ignore[assignment]
            definiteness = SucDefiniteness(msds[2].replace("+", "/")) if msds[2] != "-" else None  # type: ignore[assignment]
            pronoun_form = SucPronounForm(msds[3].replace("+", "/"))
        except ValueError as exc:
            raise UnsupportedValueError from exc
        return MsdWPos.pos_pn(
            pos=pos, gender=gender, number=number, definiteness=definiteness, pronoun_form=pronoun_form
        )
    if pos == SucPos.PS:
        gender = SucGender(msds[0].replace("+", "/"))
        number = SucNumber(msds[1].replace("+", "/"))
        definiteness = SucDefiniteness(msds[2].replace("+", "/"))
        return MsdWPos.pos_ps(pos=pos, gender=gender, number=number, definiteness=definiteness)
    if pos == SucPos.RG:
        case = SucCase(msds[0])
        return MsdWPos.pos_rg(pos=pos, case=case)
    if pos == SucPos.RO:
        try:
            gender = SucGender(msds[0].replace("+", "/")) if msds[0] != "-" else None  # type: ignore[assignment]
            number = SucNumber(msds[1].replace("+", "/")) if msds[1] != "-" else None  # type: ignore[assignment]
            definiteness = SucDefiniteness(msds[2].replace("+", "/")) if msds[2] != "-" else None  # type: ignore[assignment]
            case = SucCase(msds[3]) if msds[3] != "-" else None  # type: ignore[assignment]
        except ValueError as exc:
            try:
                case = SucCase(msds[0])
                return MsdWPos.pos_ro(pos=pos, gender=None, number=None, definiteness=None, case=case)
            except ValueError as exc:
                raise UnsupportedValueError from exc
            raise UnsupportedValueError from exc
        return MsdWPos.pos_ro(pos=pos, gender=gender, number=number, definiteness=definiteness, case=case)

    if pos == SucPos.VB:
        try:
            tense = SucTense(msds[0])
            mood = None
        except ValueError:
            try:
                mood = SucMood(msds[0])
                tense = SucTense(msds[1])
            except ValueError as exc:
                raise UnsupportedValueError from exc
        try:
            voice = SucVoice(msds[2]) if mood else SucVoice(msds[1])
        except ValueError as exc:
            raise UnsupportedValueError from exc
        return MsdWPos.pos_vb(pos=pos, tense=tense, mood=mood, voice=voice)
    return MsdWPos.with_pos(pos=pos)


class UnsupportedValueError(Exception):
    """Unsupported value for a MSD."""


def _parse_from_delimiter(delimiter: SucDelimiter, _msds: list[str]) -> Msd:
    return MsdWDelimiter(delimiter=delimiter)
