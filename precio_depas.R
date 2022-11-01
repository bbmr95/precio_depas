library(tidyverse)
library(rvest)
library(stringr)

f = ''
t = 0
c = data.frame(x = NA) %>% filter(is.na(x) == F)

while (tryCatch({ifelse(typeof(read_html(paste0("https://urbania.pe/buscar/alquiler-de-departamentos?bedroomMin=0&bedroomMax=1",f))) == 'list','iii','ti')}, error = function(e) {return('ti')}) != 'ti') {
  if (t != 0) {
    f = paste0('&page=',t + 1)
  } else {
    f = ''
  }

  c = rbind(c,
            x = data.frame(
              paste0('https://urbania.pe',read_html(paste0("https://urbania.pe/buscar/alquiler-de-departamentos?bedroomMin=0&bedroomMax=1&sort=high_price",f)) %>%
              html_elements('.cYmZqs') %>%
              html_attr('data-to-posting'))))
  t = t + 1
}

k = data.frame()
for (i in 1:10) {
  c_link = read_html(c[i,1])
  direc = paste(c_link %>% html_elements('.title-location') %>% html_text(trim = T), collapse = ' ')
  precio = paste(c_link %>% html_elements('.price-items') %>% html_text(trim = T), collapse = ' ')
  manten = paste(c_link %>% html_elements('.block-expensas') %>% html_text(trim = T), collapse = ' ')
  info_ge = paste(c_link %>% html_elements('.icon-feature') %>% html_text(trim = T), collapse = ' ')
  k = rbind(k, c(direc,precio,manten,info_ge,c[i,1]))
}

k





# Valores: 5 6 7 8 9 10
sort(unique(c$separador))

c_nuevo = c %>%
  separate(x, c('a','b','c','d','e','f','g','h','i','j','k'), sep = '\n') %>%
  mutate(precio  = as.numeric(gsub('S/|\\,','',a)),
         mant    = ifelse(grepl('\\d\\sMantenimiento',b) == T, b,
                          ifelse(grepl('\\d\\sMantenimiento',c) == T, c,
                                 ifelse(grepl('\\d\\sMantenimiento',d) == T, d,
                                        ifelse(grepl('\\d\\sMantenimiento',e) == T, e,
                                               ifelse(grepl('\\d\\sMantenimiento',f) == T, f,
                                                      ifelse(grepl('\\d\\sMantenimiento',g) == T, g,
                                                             ifelse(grepl('\\d\\sMantenimiento',h) == T, h, NA))))))),
         p_dolar = ifelse(grepl('USD',b) == T, b,
                          ifelse(grepl('USD',c) == T, c,
                                 ifelse(grepl('USD',d) == T, d,
                                        ifelse(grepl('USD',e) == T, e,
                                               ifelse(grepl('USD',a) == T, a,
                                                      ifelse(grepl('USD',f) == T, f,
                                                             ifelse(grepl('USD',g) == T, g,
                                                                    ifelse(grepl('USD',h) == T, h, NA)))))))),
         desc    = ifelse(grepl('^\\d+\\%$',b) == T, b,
                          ifelse(grepl('^\\d+\\%$',c) == T, c,
                                 ifelse(grepl('^\\d+\\%$',d) == T, d,
                                        ifelse(grepl('^\\d+\\%$',e) == T, e,
                                               ifelse(grepl('^\\d+\\%$',f) == T, f,
                                                      ifelse(grepl('^\\d+\\%$',g) == T, g,
                                                             ifelse(grepl('^\\d+\\%$',h) == T, h, NA))))))),
         spec    = ifelse(grepl('m²|dorm.|baño',c) == T, c,
                          ifelse(grepl('m²|dorm.|baño',d) == T, d,
                                 ifelse(grepl('m²|dorm.|baño',e) == T, e,
                                        ifelse(grepl('m²|dorm.|baño',f) == T, f,
                                               ifelse(grepl('m²|dorm.|baño',g) == T, g,
                                                      ifelse(grepl('m²|dorm.|baño',h) == T, h, NA)))))),
         ubiq    = ifelse(grepl('m²|dorm.|baño',c) == T, b,
                          ifelse(grepl('m²|dorm.|baño',d) == T, c,
                                 ifelse(grepl('m²|dorm.|baño',e) == T, d,
                                        ifelse(grepl('m²|dorm.|baño',f) == T, e,
                                               ifelse(grepl('m²|dorm.|baño',g) == T, f,
                                                      ifelse(grepl('m²|dorm.|baño',h) == T, g, NA))))))
  ) %>%
  select(precio,p_dolar,mant,desc,spec,ubiq,separador) %>%
  mutate(spec = gsub('\\s*estac.*','estac',gsub('\\s*baños*','banos',gsub('\\s*dorm.','dorm',gsub('(\\d+)\\sm²(\\s\\d+)\\sm²(\\s*)','\\1m_tot\\2m_tech\\3',spec))))) %>%
  separate(spec, c('a','b','c','d','e'), sep = '\\s') %>%
  mutate(m_tot = ifelse(grepl('m_tot',a) == T, gsub('m_tot','',a),
                        ifelse(grepl('m_tot',b) == T, gsub('m_tot','',b),
                               ifelse(grepl('m_tot',c) == T, gsub('m_tot','',c),
                                      ifelse(grepl('m_tot',d) == T, gsub('m_tot','',d),
                                             ifelse(grepl('m_tot',e) == T, gsub('m_tot','',e), NA))))),
         m_tech = ifelse(grepl('m_tech',a) == T, gsub('m_tech','',a),
                         ifelse(grepl('m_tech',b) == T, gsub('m_tech','',b),
                                ifelse(grepl('m_tech',c) == T, gsub('m_tech','',c),
                                       ifelse(grepl('m_tech',d) == T, gsub('m_tech','',d),
                                              ifelse(grepl('m_tech',e) == T, gsub('m_tech','',e), NA))))),
         dorm = ifelse(grepl('dorm',a) == T, gsub('dorm','',a),
                       ifelse(grepl('dorm',b) == T, gsub('dorm','',b),
                              ifelse(grepl('dorm',c) == T, gsub('dorm','',c),
                                     ifelse(grepl('dorm',d) == T, gsub('dorm','',d),
                                            ifelse(grepl('dorm',e) == T, gsub('dorm','',e), NA))))),
         banos = ifelse(grepl('banos',a) == T, gsub('banos','',a),
                        ifelse(grepl('banos',b) == T, gsub('banos','',b),
                               ifelse(grepl('banos',c) == T, gsub('banos','',c),
                                      ifelse(grepl('banos',d) == T, gsub('banos','',d),
                                             ifelse(grepl('banos',e) == T, gsub('banos','',e), NA))))),
         estac = ifelse(grepl('estac',a) == T, gsub('estac','',a),
                        ifelse(grepl('estac',b) == T, gsub('estac','',b),
                               ifelse(grepl('estac',c) == T, gsub('estac','',c),
                                      ifelse(grepl('estac',d) == T, gsub('estac','',d),
                                             ifelse(grepl('estac',e) == T, gsub('estac','',e), NA)))))
  ) %>%
  select(-c(a,b,c,d,e)) %>%
  mutate(dorm = ifelse(is.na(dorm) == T, 0, dorm),
         banos = ifelse(is.na(banos) == T, 0, banos),
         estac = ifelse(is.na(estac) == T, 0, estac),
         ubiq = toupper(iconv(ubiq, to = 'ASCII//TRANSLIT')),
         precio = as.numeric(precio),
         m_tot = as.numeric(m_tot))



dis_distritos = data.frame(
match_distritos = c('SAN ISIDRO','ANCON','ATE','BARRANCO','CARABAYLLO','CERCADO DE LIMA','CHACLACAYO','CHORRILLOS','CIENEGUILLA','COMAS','EL AGUSTINO',
              'INDEPENDENCIA','JESUS MARIA','LA MOLINA','LA VICTORIA','LINCE','LOS OLIVOS','LURIGANCHO','LURIN','MAGADALENA DEL MAR','MIRAFLORES',
              'PACHACAMAC','PUCUSANA','PUEBLO LIBRE','PUENTE PIEDRA','PUNTA HERMOSA','PUNTA NEGRA','RIMAC','SAN BARTOLO','SAN BORJA','SAN JUAN DE LURIGANCHO',
              'SAN JUAN DE MIRAFLORES','SAN LUIS','SAN MARTIN DE PORRES','SAN MIGUEL','SANTA ANITA','SANTA MARIA DEL MAR','SANTA ROSA','SANTIAGO DE SURCO',
              'SURQUILLO','VILLA EL SALVADOR','VILLA MARIA DEL TRIUNFO', 'BRENA',
              'VENTANILLA','BELLAVISTA','CALLAO','LA PERLA','LA PUNTA','MI PERU','CARMEN DE A LEGUA', 'LIMA CERCADO', 'MAGDALENA'),
distritos = c('SAN ISIDRO','ANCON','ATE','BARRANCO','CARABAYLLO','CERCADO DE LIMA','CHACLACAYO','CHORRILLOS','CIENEGUILLA','COMAS','EL AGUSTINO',
              'INDEPENDENCIA','JESUS MARIA','LA MOLINA','LA VICTORIA','LINCE','LOS OLIVOS','LURIGANCHO','LURIN','MAGADALENA DEL MAR','MIRAFLORES',
              'PACHACAMAC','PUCUSANA','PUEBLO LIBRE','PUENTE PIEDRA','PUNTA HERMOSA','PUNTA NEGRA','RIMAC','SAN BARTOLO','SAN BORJA','SAN JUAN DE LURIGANCHO',
              'SAN JUAN DE MIRAFLORES','SAN LUIS','SAN MARTIN DE PORRES','SAN MIGUEL','SANTA ANITA','SANTA MARIA DEL MAR','SANTA ROSA','SANTIAGO DE SURCO',
              'SURQUILLO','VILLA EL SALVADOR','VILLA MARIA DEL TRIUNFO', 'BREÑA',
              'VENTANILLA','BELLAVISTA','CALLAO','LA PERLA','LA PUNTA','MI PERU','CARMEN DE A LEGUA', 'CERCADO DE LIMA', 'MAGDALENA DEL MAR')
)

c_nuevo = c_nuevo %>% mutate(match_dis = NA)
for (dis in 1:nrow(dis_distritos)) {
  c_nuevo = c_nuevo %>%
    mutate(match_dis = ifelse(grepl(dis_distritos$match_distritos[dis],ubiq) == T, dis_distritos$distritos[dis], match_dis))
}

View(
  c_nuevo %>%
    mutate(precio = as.numeric(precio), m_tot = as.numeric(m_tot)) %>%
    filter(is.na(match_dis) == F, dorm == 1, is.na(precio) == F, m_tot > 1) %>%
    group_by(match_dis) %>%
    summarise(sum_p = sum(precio)/sum(m_tot))
)










