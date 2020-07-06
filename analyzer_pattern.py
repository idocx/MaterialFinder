hydrocarbon_suffixes = (
    # "al",
    # "ane",
    "ate",
    # "ase",
    # "ene",
    # "ide"
    "ine",
    # "ite"
    # "oate",
    # "oic",
    # "ol",
    # "one",
    # "ose",
    # "yne",
    "yl",
)

organic_prefixes = (
    # "meth-",
    # "eth-",
    # "prop-",
    # "but-",
    # "pent-",
    # "hex-",
    # "hept-",
    # "oct-",
    # "non-",
    # "dec-",
    # "undec-",
    # "dodec-",
    # "tridec-",
    # "tetradec-",
    # "pentadec-",
    # "hexadec-",
    # "heptadec-",
    # "octadec-",
    # "nonadec-",
    # "eicosan-",
    "fluoro",
    "chloro",
    "bromo",
    "iodo",
)

special_groups = (
    "poly",
)

pattern = r"(((?<={suffixes})|(?={prefixes})|(?<={special_group})|(?={special_group}))|\W)+".format(
    suffixes="|".join(hydrocarbon_suffixes),
    prefixes="|".join(organic_prefixes),
    special_group="|".join(special_groups)
)


if __name__ == '__main__':
    print(pattern)
