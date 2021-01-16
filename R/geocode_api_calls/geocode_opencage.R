library(opencage)
Sys.setenv(OPENCAGE_KEY = "3c4142acfd9f41afa7f63d180a8fc4d0")

library(tidyverse)
library(opencage)

#loadd(locais)
#addr <- select(locais, nm_locvot, ds_endereco,
# ds_bairro, nm_localidade, sg_uf) %>%
# distinct()
#addr$search_string <- stringr::str_squish(paste0(addr$ds_endereco, ", ",
#      addr$ds_bairro, ", ", addr$nm_localidade, ", ", addr$sg_uf, ", Brasil"))
#addr$confidence <- NA
#addr$opencage_lat <- NA
#addr$opencage_long <- NA
#readr::write_csv(addr, file = "./data/opencage_geocodes.csv.gz")


addr <- vroom::vroom(file = "./data/opencage_geocodes.csv.gz")
for(i in 1:nrow(addr)){
  if (is.na(addr$confidence[i]) == FALSE) next
  print(i)
  opencage_result <- opencage_forward(placename = addr$search_string[i],
                                      countrycode = "BR", language = "pt-BR",
                                      no_annotations = TRUE)
  if (is.null(opencage_result$results)) next
  #remaining <- opencage_result$rate_info$remaining
  #if (remaining < 1) break
  opencage_lnglat <- opencage_result$results %>%
    slice_max(confidence) %>%
    slice_head(n = 1) %>%
    select(confidence, geometry.lat, geometry.lng)
  addr$opencage_lat[i] <- opencage_lnglat$geometry.lat
  addr$opencage_long[i] <- opencage_lnglat$geometry.lng
  addr$confidence[i] <- opencage_lnglat$confidence
  Sys.sleep(.07)
}
vroom::vroom_write(addr, "./data/opencage_geocodes.csv.gz", delim = ",")
