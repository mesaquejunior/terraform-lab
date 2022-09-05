select avg(nu_nota_ch) from enem_2020 where sg_uf_esc = 'SP' and tp_sexo = 'M';

select no_municipio_esc, avg(nu_nota_mt) as media from enem_2020 group by no_municipio_esc order by media desc;

select count(nu_inscricao) from enem_2020 where no_municipio_prova = 'Recife' and no_municipio_esc = 'Recife';

select avg(nu_nota_ch) from enem_2020 where sg_uf_esc = 'SC' and q008 != 'A';

select avg(nu_nota_mt) from enem_2020 where q002 in ('F', 'G') and tp_sexo = 'F' and no_municipio_esc = 'Belo Horizonte';

select * from enem_2020 limit 10;