// ============================================================================
// Keyword-to-Hazard Matching System (JavaScript port)
// Built from analysis of 134 RAMS documents with 5342 method steps
// and 1772 hazard selections.
// ============================================================================

const KEYWORD_HAZARD_MAP = {
    // ==================== POWER TOOLS & EQUIPMENT ====================
    "grinder": ["Angle Grinders", "Hand-arm Vibration", "Hot Work", "Flying / Ejected Objects", "Sharp Edges", "Airborne Dust", "Hazardous Noise Levels", "Portable Electrical Equipment", "Metal striking activities - Hardened Steel against hardened steel"],
    "grinding": ["Angle Grinders", "Hand-arm Vibration", "Hot Work", "Flying / Ejected Objects", "Airborne Dust", "Metal striking activities - Hardened Steel against hardened steel"],
    "angle grinder": ["Angle Grinders", "Hand-arm Vibration", "Hot Work", "Flying / Ejected Objects", "Sharp Edges", "Hazardous Noise Levels", "Metal striking activities - Hardened Steel against hardened steel"],
    "hammer": ["Metal striking activities - Hardened Steel against hardened steel", "Flying / Ejected Objects", "Hand-arm Vibration", "Hazardous Noise Levels"],
    "sledgehammer": ["Metal striking activities - Hardened Steel against hardened steel", "Flying / Ejected Objects", "Hand-arm Vibration", "Hazardous Noise Levels", "Manual Handling"],
    "chipping hammer": ["Metal striking activities - Hardened Steel against hardened steel", "Flying / Ejected Objects", "Hand-arm Vibration"],
    "striking": ["Metal striking activities - Hardened Steel against hardened steel", "Flying / Ejected Objects"],
    "drill": ["110v Drills & Magnetic Base Drills", "Hand-arm Vibration", "Rotating Equipment", "Portable Electrical Equipment", "Airborne Dust"],
    "drilling": ["110v Drills & Magnetic Base Drills", "Hand-arm Vibration", "Airborne Dust"],
    "hilti": ["Use of Jack Hammers / Hilti Drills", "Hand-arm Vibration", "Hazardous Noise Levels", "Airborne Dust"],
    "jack hammer": ["Use of Jack Hammers / Hilti Drills", "Hand-arm Vibration", "Hazardous Noise Levels"],
    "impact wrench": ["Hand-arm Vibration", "Hazardous Noise Levels", "Pneumatic Tools"],
    "pneumatic": ["Pneumatic Tools", "Hazardous Noise Levels", "Hand-arm Vibration"],

    // ==================== WELDING ====================
    "weld": ["Welding", "Welding Sets", "Hot Work", "Hand Tools", "Sharp Edges", "Airborne Dust", "Hand-arm Vibration"],
    "welding": ["Welding", "Welding Sets", "Hot Work", "Hand Tools", "Sharp Edges", "Airborne Dust", "Hand-arm Vibration"],
    "mig": ["Welding", "Welding Sets", "Hot Work", "Compressed Gas Cylinders"],
    "tig": ["Welding", "Welding Sets", "Hot Work", "Hand-arm Vibration", "Airborne Dust", "Angle Grinders"],
    "arc": ["Welding", "Welding Sets", "Hot Work", "Electrical Power"],
    "oxy": ["Hot Work", "Compressed Gas Cylinders", "Oxygen", "Flying / Ejected Objects"],
    "burning": ["Hot Work", "Welding Sets", "Hand Tools", "Airborne Dust"],
    "gouging": ["Air Arcing gouging / Lancing", "Hot Work", "Hazardous Noise Levels", "Airborne Dust"],
    "lancing": ["Air Arcing gouging / Lancing", "Hot Work"],
    "hot work": ["Hot Work", "Welding", "Welding Sets", "Sharp Edges", "Hand Tools", "Airborne Dust"],

    // ==================== LIFTING ====================
    "crane": ["Cranes (Mobile, Overhead Etc)", "Lift Operations", "Objects falling from above"],
    "lift": ["Lift Operations", "Manual Handling", "Hand Tools", "Objects falling from above", "Trapping / Pinch Points"],
    "lifting": ["Lift Operations", "Manual Handling", "Hand Tools", "Objects falling from above", "Trapping / Pinch Points", "Hand-arm Vibration"],
    "hoist": ["Lift Operations", "Objects falling from above", "Trapping / Pinch Points"],
    "chain block": ["Lift Operations", "Manual Handling", "Trapping / Pinch Points"],
    "sling": ["Lift Operations", "Objects falling from above", "Sharp Edges"],
    "forklift": ["Fork Lift Trucks (Including Telehandlers)", "Heavy plant and Road Traffic", "Objects falling from above"],
    "telehandler": ["Fork Lift Trucks (Including Telehandlers)", "Heavy plant and Road Traffic", "Objects falling from above", "Lift Operations"],
    "gantry": ["Use of Mobile Lifting Frames/ Portable Gantries", "Lift Operations", "Objects falling from above"],

    // ==================== ACCESS EQUIPMENT ====================
    "ladder": ["Use of Ladders", "Working at Height", "Objects falling from above", "Slips, Trips and Falls / Poor Housekeeping"],
    "stepladder": ["Use of Stepladders", "Working at Height", "Slips, Trips and Falls / Poor Housekeeping"],
    "scaffold": ["Scaffold", "Working at Height", "Objects falling from above", "Manual Handling", "Hand Tools", "Hand-arm Vibration", "Hot Work"],
    "scaffolding": ["Scaffold", "Working at Height", "Objects falling from above"],
    "mewp": ["MEWPS", "Working at Height", "Poor Ground Conditions", "Trapping / Pinch Points", "Hand Tools", "Manual Handling"],
    "cherry picker": ["MEWPS", "Working at Height", "Flying / Ejected Objects", "Hand Tools", "Hand-arm Vibration"],
    "boom lift": ["MEWPS", "Working at Height", "Poor Ground Conditions"],
    "scissor lift": ["MEWPS", "Working at Height", "Trapping / Pinch Points"],
    "harness": ["Use of Harnesses", "Working at Height", "Unauthorised or Inadequate Access and Egress", "Poor Ground Conditions", "Other Working Parties in the Area"],
    "height": ["Working at Height", "Objects falling from above", "Use of Harnesses"],
    "elevated": ["Working at Height", "MEWPS", "Objects falling from above"],
    "platform": ["Work at height: existing platforms and walkways", "Working at Height", "Objects falling from above"],

    // ==================== CONFINED SPACES ====================
    "confined space": ["Confined Spaces", "Manual Handling", "Hot Work", "Welding Sets", "Hand Tools", "Work at height: existing platforms and walkways"],
    "vessel": ["Confined Spaces", "Stored Energy", "Pressure Systems"],
    "tank": ["Confined Spaces", "Stored Energy", "Hazardous Process Gasses"],

    // ==================== ELECTRICAL ====================
    "electrical": ["Electrical Power", "Portable Electrical Equipment", "Stored Energy", "Arc Flash / Arc Blast", "Electrical Live Testing"],
    "arc flash": ["Arc Flash / Arc Blast", "Electrical Power"],
    "arc blast": ["Arc Flash / Arc Blast", "Electrical Power"],
    "energised": ["Arc Flash / Arc Blast", "Electrical Power", "Electrical Live Testing"],
    "live testing": ["Electrical Live Testing", "Electrical Power", "Arc Flash / Arc Blast"],
    "electrical testing": ["Electrical Live Testing", "Electrical Power"],
    "test meter": ["Electrical Live Testing", "Electrical Power"],
    "multimeter": ["Electrical Live Testing", "Electrical Power"],
    "gs38": ["Electrical Live Testing", "Electrical Power"],
    "prove test prove": ["Electrical Live Testing", "Electrical Power"],
    "isolation": ["Electrical Power", "Stored Energy", "Manual Handling", "Hand Tools"],
    "isolat": ["Electrical Power", "Stored Energy", "Hand Tools"],
    "loto": ["Electrical Power", "Stored Energy"],
    "lockout": ["Electrical Power", "Stored Energy"],

    // ==================== CUTTING ====================
    "cut": ["Sharp Edges", "Angle Grinders", "Hand Tools", "Hand-arm Vibration", "Manual Handling", "Airborne Dust"],
    "cutting": ["Sharp Edges", "Angle Grinders", "Hand Tools", "Hot Work", "Airborne Dust", "Chop Saw (Abrasive Cut-Off Saw)"],
    "chop saw": ["Chop Saw (Abrasive Cut-Off Saw)", "Sharp Edges", "Hot Work", "Hazardous Noise Levels", "Flying / Ejected Objects", "Airborne Dust"],
    "cut off saw": ["Chop Saw (Abrasive Cut-Off Saw)", "Sharp Edges", "Hot Work", "Hazardous Noise Levels", "Flying / Ejected Objects"],
    "abrasive saw": ["Chop Saw (Abrasive Cut-Off Saw)", "Sharp Edges", "Hot Work", "Hazardous Noise Levels", "Flying / Ejected Objects"],
    "metal saw": ["Chop Saw (Abrasive Cut-Off Saw)", "Sharp Edges", "Hazardous Noise Levels", "Airborne Dust"],
    "drop saw": ["Chop Saw (Abrasive Cut-Off Saw)", "Sharp Edges", "Hot Work", "Hazardous Noise Levels"],
    "plasma": ["Hot Work", "Sharp Edges", "Electrical Power", "Hazardous Noise Levels"],
    "knife": ["Use of Knives/ Blades", "Sharp Edges"],
    "blade": ["Use of Knives/ Blades", "Sharp Edges", "Rotating Equipment"],

    // ==================== MANUAL HANDLING ====================
    "manual handling": ["Manual Handling", "Hydraulic Power", "Sharp Edges"],
    "heavy": ["Manual Handling", "Hand Tools", "Trapping / Pinch Points"],
    "bolt": ["Manual Handling", "Hand Tools", "Sharp Edges", "Hand-arm Vibration", "Trapping / Pinch Points", "Airborne Dust", "Poor Ground Conditions"],
    "torque": ["Manual Handling", "Hand Tools", "Trapping / Pinch Points"],
    "tension": ["Manual Handling", "Hand Tools", "Hand-arm Vibration", "Fork Lift Trucks (Including Telehandlers)"],

    // ==================== PIPEWORK & PRESSURE ====================
    "pipe": ["Manual Handling", "Hot Work", "Welding Sets", "Hand Tools", "Pressure Systems", "Pipework - Residual Contents / Drain Down", "Pipework - Cutting and Threading", "Pipework - Heavy Sections / Manual Handling", "Pipework - Flanged Joint Assembly"],
    "pipework": ["Manual Handling", "Hot Work", "Welding Sets", "Hand Tools", "Work at height: existing platforms and walkways", "Pipework - Residual Contents / Drain Down", "Pipework - Cutting and Threading", "Pipework - Heavy Sections / Manual Handling", "Pipework - Flanged Joint Assembly"],
    "pressure": ["Pressure Systems", "Stored Energy", "Hydraulic Power"],
    "hydraulic": ["Hydraulic Power", "Pressure Systems", "Stored Energy", "Hand Tools"],
    "valve": ["Pressure Systems", "Stored Energy", "Hand Tools", "Manual Handling"],
    "pump": ["Rotating Equipment", "Electrical Power", "Manual Handling", "Pressure Systems"],
    "flange": ["Pipework - Flanged Joint Assembly", "Pipework - Residual Contents / Drain Down"],
    "gasket": ["Pipework - Flanged Joint Assembly"],
    "threading": ["Pipework - Cutting and Threading"],
    "drain down": ["Pipework - Residual Contents / Drain Down"],
    "residual": ["Pipework - Residual Contents / Drain Down"],
    "pipe cutting": ["Pipework - Cutting and Threading"],
    "pipe vice": ["Pipework - Cutting and Threading"],
    "torque wrench": ["Pipework - Flanged Joint Assembly"],

    // ==================== SUBSTANCES & ENVIRONMENT ====================
    "asbestos": ["Asbestos", "Manual Handling", "Hot Work", "Hand Tools"],
    "dust": ["Airborne Dust", "Angle Grinders", "Hand Tools", "Hand-arm Vibration"],
    "chemical": ["Hazardous Substances Fuels, Paints, Lubrications etc.", "Process Acids"],
    "paint": ["Hazardous Substances Fuels, Paints, Lubrications etc.", "Airborne Dust"],
    "oil": ["Oil and Grease", "Fork Lift Trucks (Including Telehandlers)", "Leaks, drips and spillages (Environmental hazard)"],
    "grease": ["Oil and Grease", "Hand Tools", "Slips, Trips and Falls / Poor Housekeeping"],
    "fire": ["Hot Work", "Sharp Edges", "Airborne Dust", "Hand Tools", "Manual Handling", "Poor Lighting"],
    "sparks": ["Hot Work", "Angle Grinders", "Welding", "Flying / Ejected Objects"],

    // ==================== EXCAVATION ====================
    "excavat": ["Excavations", "Poor Ground Conditions", "Overhead Power"],
    "dig": ["Excavations", "Manual Handling"],
    "trench": ["Excavations", "Poor Ground Conditions"],

    // ==================== VEHICLE & TRAFFIC ====================
    "traffic": ["Heavy plant and Road Traffic", "On-Site Driving"],
    "vehicle": ["Heavy plant and Road Traffic", "On-Site Driving", "Loading / Unloading Activities"],
    "driving": ["On-Site Driving", "Heavy plant and Road Traffic"],

    // ==================== ENVIRONMENT & CONDITIONS ====================
    "noise": ["Hazardous Noise Levels", "Hand-arm Vibration"],
    "vibration": ["Hand-arm Vibration", "Hazardous Noise Levels"],
    "weather": ["Inclement Weather", "Working at Height"],
    "hot": ["Extreme Temperatures", "Hot Work"],
    "cold": ["Extreme Temperatures", "Inclement Weather"],
    "lighting": ["Poor Lighting", "Electrical Power"],
    "ground": ["Poor Ground Conditions", "Excavations"],
    "housekeeping": ["Slips, Trips and Falls / Poor Housekeeping", "Objects falling from above"],
    "gas": ["Hazardous Process Gasses", "Compressed Gas Cylinders", "Confined Spaces"],
    "cylinder": ["Compressed Gas Cylinders", "Hazardous Process Gasses"],

    // ==================== JACKS & SUPPORTS ====================
    "jack": ["Use of Jacks (Hydraulic etc.)", "Manual Handling", "Trapping / Pinch Points"],
    "jacking": ["Use of Jacks (Hydraulic etc.)", "Manual Handling"],

    // ==================== MACHINERY ====================
    "machinery": ["Use of Machinery", "Rotating Equipment", "Trapping / Pinch Points", "Stored Energy"],
    "rotating": ["Rotating Equipment", "Trapping / Pinch Points", "Stored Energy"],
    "motor": ["Rotating Equipment", "Electrical Power", "Stored Energy"],
    "conveyor": ["Rotating Equipment", "Trapping / Pinch Points", "Stored Energy"],
};

// Synonyms that map to primary keywords
const KEYWORD_SYNONYMS = {
    // Grinding
    "disc cutter": "grinder", "cutting disc": "grinder", "grind": "grinder",
    // Hammers
    "ball pein hammer": "hammer", "ball peen hammer": "hammer", "lump hammer": "hammer",
    "club hammer": "hammer", "rubber mallet": "hammer", "dead blow hammer": "hammer",
    "copper hammer": "hammer", "pin hammer": "hammer", "sledge hammer": "sledgehammer",
    "slag hammer": "chipping hammer", "chipping": "chipping hammer",
    // Welding
    "mma": "weld", "stick welding": "weld", "mig welding": "mig", "tig welding": "tig",
    "arc welding": "arc", "oxy-fuel": "oxy", "oxy acetylene": "oxy", "oxy-acetylene": "oxy",
    "gas cutting": "oxy", "flame cutting": "oxy",
    // Drilling
    "core drill": "drill", "hammer drill": "drill", "magnetic drill": "drill", "mag drill": "drill",
    // Arc Flash / Electrical Testing
    "flash hazard": "arc flash", "incident energy": "arc flash", "ppe boundary": "arc flash",
    "cal cm2": "arc flash", "arcflash": "arc flash", "arc-flash": "arc flash",
    "flash burn": "arc flash", "electrical burn": "arc flash",
    "electric shock test": "live testing", "testing live": "live testing", "voltage test": "live testing",
    "circuit test": "electrical testing", "continuity test": "electrical testing",
    "insulation test": "electrical testing", "meg test": "electrical testing",
    "megger": "electrical testing", "fluke meter": "multimeter",
    // Chop Saw
    "abrasive wheel": "chop saw", "cutoff wheel": "chop saw", "cut-off wheel": "chop saw",
    "metal cutting saw": "chop saw", "steel saw": "chop saw", "abrasive cutoff": "chop saw",
    "abrasive cut-off": "chop saw",
    // Lifting
    "mobile crane": "crane", "tower crane": "crane", "crawler crane": "crane",
    "overhead crane": "crane", "gantry crane": "crane", "tirfor": "chain block",
    "lever hoist": "chain block", "pull lift": "chain block", "flt": "forklift",
    "fork truck": "forklift", "tele-handler": "telehandler", "telescopic handler": "telehandler",
    "lifting frame": "gantry", "a-frame": "gantry", "lifting beam": "sling", "shackle": "sling",
    // Access
    "ewp": "mewp", "elevated work platform": "mewp", "ipaf": "mewp",
    "step ladder": "stepladder", "podium steps": "stepladder", "podium": "stepladder",
    "tower scaffold": "scaffold", "tube and fitting": "scaffold",
    "fall arrest": "harness", "fall restraint": "harness", "lanyard": "harness",
    "inertia reel": "harness", "working at height": "height", "work at height": "height",
    "above ground": "height",
    // Confined spaces
    "cse": "confined space", "entry permit": "confined space", "enclosed space": "confined space",
    // Electrical
    "lock-out": "loto", "tagout": "loto", "tag-out": "loto", "lock out tag out": "loto",
    "safe isolation": "isolation", "isolate": "isolation",
    // Cutting
    "plasma cutter": "plasma", "plasma cutting": "plasma",
    "stanley knife": "knife", "utility knife": "knife",
    // Substances
    "hazardous substance": "chemical", "coshh": "chemical",
    "painting": "paint", "spray paint": "paint", "coating": "paint",
    "acm": "asbestos", "asbestos containing": "asbestos",
    "dusty": "dust", "particles": "dust", "silica": "dust",
    // Pipework
    "piping": "pipe", "pipeline": "pipe", "pipefitting": "pipe",
    "pipe work": "pipework", "pipe-work": "pipework",
    "flanged": "flange", "flange joint": "flange", "bolted joint": "flange",
    "bolt up": "flange", "jointing": "flange",
    "gaskets": "gasket", "spiral wound": "gasket", "klingersil": "gasket",
    "pipe thread": "threading", "bspt": "threading", "npt": "threading", "threaded joint": "threading",
    "draining": "drain down", "draindown": "drain down", "empty pipe": "drain down",
    "depressurise": "drain down", "depressurize": "drain down",
    "residual fluid": "residual", "residual contents": "residual",
    "trapped contents": "residual", "breaking containment": "residual",
    "pipe cutter": "pipe cutting", "pipe threader": "pipe cutting",
    "ridgid": "pipe cutting", "rothenberger": "pipe cutting",
    "bolting": "flange", "fastener": "flange", "nut": "flange",
    "torquing": "torque wrench", "bolt tensioning": "torque wrench", "flange torque": "torque wrench",
    // Excavation
    "digging": "dig", "trenching": "trench", "excavation": "excavat",
    // Traffic
    "road": "traffic", "transport": "vehicle", "haulage": "vehicle",
    // Environment
    "noisy": "noise", "loud": "noise", "havs": "vibration",
};

/**
 * KeywordHazardMatcher - Matches text to relevant hazards using keyword analysis.
 * Port of the Python KeywordHazardMatcher from RAMSBuilder desktop app.
 */
class KeywordHazardMatcher {
    constructor() {
        this.keywordIndex = {};
        this._buildIndex();
    }

    _buildIndex() {
        // Primary keywords
        for (const kw of Object.keys(KEYWORD_HAZARD_MAP)) {
            this.keywordIndex[kw.toLowerCase()] = kw;
        }
        // Synonyms
        for (const [syn, primary] of Object.entries(KEYWORD_SYNONYMS)) {
            this.keywordIndex[syn.toLowerCase()] = primary;
        }
    }

    /**
     * Match text against keyword database and return relevant hazards.
     * @param {string} text - Text to analyze (scope, method steps, etc.)
     * @returns {Array<{hazard_name: string, matched_keywords: string[], confidence: number}>}
     */
    matchText(text) {
        if (!text) return [];
        const normalized = text.toLowerCase();

        // Extract matched keywords
        const matchedPrimaries = new Set();
        const matchedOriginals = {};

        for (const [term, primary] of Object.entries(this.keywordIndex)) {
            let found = false;
            if (term.length <= 4) {
                // Word boundary match for short terms
                const re = new RegExp('\\b' + term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b');
                found = re.test(normalized);
            } else {
                found = normalized.includes(term);
            }
            if (found) {
                matchedPrimaries.add(primary);
                if (!matchedOriginals[primary]) matchedOriginals[primary] = [];
                matchedOriginals[primary].push(term);
            }
        }

        if (matchedPrimaries.size === 0) return [];

        // Accumulate hazards
        const hazardAcc = {};
        for (const keyword of matchedPrimaries) {
            const hazardNames = KEYWORD_HAZARD_MAP[keyword] || [];
            for (const hName of hazardNames) {
                if (!hazardAcc[hName]) {
                    hazardAcc[hName] = { keywords: [] };
                }
                hazardAcc[hName].keywords.push(...(matchedOriginals[keyword] || []));
            }
        }

        // Calculate confidence and build results
        const maxKw = Math.max(...Object.values(hazardAcc).map(h => new Set(h.keywords).size));
        const results = [];
        for (const [hazardName, data] of Object.entries(hazardAcc)) {
            const uniqueKw = [...new Set(data.keywords)];
            const confidence = Math.min(1.0, 0.5 + (uniqueKw.length / maxKw) * 0.5);
            results.push({ hazard_name: hazardName, matched_keywords: uniqueKw, confidence });
        }

        // Sort by confidence desc, then name
        results.sort((a, b) => b.confidence - a.confidence || a.hazard_name.localeCompare(b.hazard_name));
        return results;
    }
}

// Singleton instance
const keywordHazardMatcher = new KeywordHazardMatcher();
