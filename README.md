## step1 :- docker run --rm "debian:bullseye-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'
## step 2:- made diectories dags,logs,plugins
## step 3:- docker compose up airflow-init
## step 4:- docker compose up



## you can create new dag and keep in dag folder which you have created above