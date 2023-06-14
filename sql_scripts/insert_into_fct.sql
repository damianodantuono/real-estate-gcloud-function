INSERT INTO `real-estate-analyser-dv.dwhre.FCT_HOUSES` (
  ID,
  TITLE,
  CITY_ID,
  IS_RENT,
  PRICE,
  SURFACE,
  ROOMS,
  BATHROOMS,
  FLOOR,
  LINK
)
SELECT
ID,
TITLE,
CITY_ID,
IS_RENT,
PRICE,
SURFACE,
ROOMS,
BATHROOMS,
FLOOR,
LINK
FROM `real-estate-analyser-dv.dwhre.EXT_HOUSES` ext
INNER JOIN `real-estate-analyser-dv.dwhre.DIM_CITY` d on ext.CITY_NAME = d.CITY_NAME
WHERE ID NOT IN (SELECT ID FROM `real-estate-analyser-dv.dwhre.FCT_HOUSES`)
;

UPDATE `real-estate-analyser-dv.dwhre.FCT_HOUSES`
SET
DELETED_FLAG = True,
INSERT_UPDATE_TIMESTAMP = current_timestamp()
WHERE ID NOT IN (SELECT ID FROM `real-estate-analyser-dv.dwhre.EXT_HOUSES`) and NOT DELETED_FLAG
;
