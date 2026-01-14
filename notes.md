FAB_Clean = 
SWITCH(
    DimTools[FAB],
    "INT.FAB34", "F34",
    "INTEL-INT32", "F32",
    "INT.FAB24", "F24",
    "INTEL-INT12C", "F12C",
    "Intel-INT42", "F42",
    "INTEL-INTD1D", "D1D",
    "INTEL-INT52", "F52",
    "INTEL-INTD1X", "D1X",
    "INTEL-INTD1C", "D1C",
    DimTools[FAB]
)
