#!/usr/bin/env bash
set -u

# ---- Adjust these two if needed ----
OUT="cache:/home/query/.local/share/bolt-launcher/Jagex/RuneScape/"
CLI="node dist/cli"
# -----------------------------------

# Helper: run one mode with its source, log result, keep going on error.
run_mode() {
  local mode="$1"
  local src="$2"
  echo "==> Extracting mode: ${mode}  (source: ${src})"
  if $CLI extract -o "$OUT" -s "$src" --mode "$mode"; then
    echo "✅  ${mode} done"
  else
    echo "⚠️  ${mode} failed (continuing)"; return 0
  fi
}

# Mode → source mapping. Tweak paths on the right if your repo uses different names.
declare -A M=(
  # Core cache majors
  [framemaps]="cache/framemaps"
  [items]="cache/items"
  [enums]="cache/enums"
  [npcs]="cache/npcs"
  [soundjson]="cache/sounds"
  [musicjson]="cache/music"
  [objects]="cache/objects"
  [achievements]="cache/achievements"
  [structs]="cache/structs"
  [sequences]="cache/sequences"
  [spotanims]="cache/spotanims"
  [materials]="cache/materials"
  [oldmaterials]="cache/materials"          # singleMinorIndex(materials,0)
  [interfaces]="cache/interfaces"

  # Config-backed
  [dbtables]="cache/config/dbtables"
  [dbrows]="cache/config/dbrows"
  [overlays]="cache/config/overlays"
  [identitykit]="cache/config/identitykit"
  [params]="cache/config/params"
  [underlays]="cache/config/underlays"
  [mapscenes]="cache/config/mapscenes"
  [environments]="cache/config/environments"
  [animgroupconfigs]="cache/config/animgroupconfigs"
  [maplabels]="cache/config/maplabels"

  # World map
  [mapzones]="cache/worldmap"
  [maptiles]="cache/maptiles"       # cacheMapFiles.squares
  [maptiles_nxt]="cache/maptiles_nxt"
  [maplocations]="cache/maplocations" # cacheMapFiles.locations
  [mapenvs]="cache/mapenvs"            # cacheMapFiles.env
  [maptiles_old]="cache/maptiles_old"     # oldWorldmapIndex("m")
  [maplocations_old]="cache/maplocations_old" # oldWorldmapIndex("l")

  # Animation / models / textures
  [frames]="cache/frames"
  [models]="cache/models"
  [oldmodels]="cache/oldmodels"
  [skeletons]="cache/skeletalanims"
  [proctextures]="cache/texturesOldPng"
  [oldproctextures]="cache/texturesOldPng"

  # Misc
  [classicmodels]="cache/classic/models"    # classicGroups.models
  [indices]="cache/index"
  [rootindex]="cache/rootindex"
  [cutscenes]="cache/cutscenes"
  [particles0]="cache/particles"
  [particles1]="cache/particles"
  [quickchatcats]="cache/quickchat"
  [quickchatlines]="cache/quickchat"
  [clientscriptops]="cache/clientscript"
  [test]="cache/any"
)

# If you want a specific order, list it here; otherwise the default iteration is fine.
ORDER=(
  framemaps
  items
  enums
  npcs
  objects
  achievements
  structs
  sequences
  spotanims
  
)

# Run all
for mode in "${ORDER[@]}"; do
  run_mode "$mode" "${M[$mode]}"
done

echo "✨ All extract jobs attempted."
