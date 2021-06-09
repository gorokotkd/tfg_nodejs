'use strict'



const { performance } = require('perf_hooks');
const fs = require('fs');
const zlib = require('zlib');
const lzma = require('lzma-native');
const lzss = require('lzbase62');

const moment = require('moment');
const cassandra = require('cassandra-driver');


var Factura = require('../model/factura');
var AgrupacionFactura = require('../model/facturaAgrupada');
var DATA = require('../functions/getData');
const createFacturas = require('../functions/createData');
const { companies_nif_list } = require('../functions/companies_nif');
const FacturaAgrupada = require('../model/facturaAgrupada');
const { db } = require('../model/factura');


const FACTURAS_AGRUPADAS_PATH = "../facturas/";
const MB = 1000000;

const GZIP_PARAMS = {
    level: 1,
    flush: zlib.constants.BROTLI_OPERATION_PROCESS,
    finishFlush: zlib.constants.BROTLI_OPERATION_FINISH,
    chunkSize: 16 * 1024
};

function compress_lzma2_file(file) {
    return new Promise((resolve) => {
        lzma.compress(file, function (result) {
            resolve(result);
        });
    });
}

function decompress_lzma2_file(file) {
    return new Promise((resolve) => {
        lzma.decompress(file, function (result) {
            resolve(result);
        });
    });
}

function compress_lzma_file(file) {
    return new Promise((resolve, reject) => {
        lzma.LZMA().compress(file, 1, (result, error) => {
            if(error){
                reject(error);
            }else{
                resolve(result);
            }
        });
    });
}

function decompress_lzma_file(file) {
    return new Promise((resolve) => {
        lzma.LZMA().decompress(file, function (result) {
            resolve(result);
        });
    });
}

function insert_mongo(data) {
    return new Promise((resolve, reject) => {
        const fact = new Factura();
        fact.collection.insertMany(data, { ordered: false }, (err, docs) => {
            if (err) { reject(err) }
            else { resolve(); }
        });
    });
}

function insert_agrupadas_mongo(data) {
    return new Promise((resolve) => {
        const group = new AgrupacionFactura();
        group.collection.insertMany(data, { ordered: false }, (err, docs) => {
            if (err) { console.log(err); }
            else { resolve("Insertadas " + docs.insertedCount + " agrupaciones"); }
        });
    });
}

function compressData(data) {
    return new Promise((resolve) => {
        zlib.gzip(data, GZIP_PARAMS, (err, result) => {
            if (!err) resolve(result.toString('base64'),
            );
        });
    });
}

function unCompressData(data) {
    return new Promise((resolve, reject) => {
        zlib.gunzip(Buffer.from(data, "base64"), GZIP_PARAMS, (err, result) => {
            if (!err) resolve(result.toString());
            reject(err);
        });
    });
}

function getEarningStats(nif, fechaIni, fechaFin) {
    return new Promise((resolve, reject) => {
        Factura.aggregate([
            {
                $match: {
                    nif: nif, fecha: {
                        $gte: fechaIni,
                        $lte: fechaFin
                    }
                }
            },
            { $group: { _id: null, total: { $sum: "$cantidad" }, media: { $avg: "$cantidad" } } }
        ], (err, resul) => {
            if (err) reject(err);
            resolve(resul);
        });
    });
}

var controller = {
    index: async function (req, res) {

        var stats_dia = await getEarningStats("28693295J", new Date("2021-03-19T00:00:00"), new Date("2021-03-19T23:59:59"));
        var stats_mes = await getEarningStats("28693295J", new Date("2021-03-01T00:00:00"), new Date("2021-03-28T23:59:59"));
        return res.status(200).render('index', { title: 'Express', page: 'index', totalDia: stats_dia[0].total, totalMes: stats_mes[0].total });

        //return res.status(200).render('index', { title: 'Express', page: 'index'});
    },
    createData: async function (req, res) {
        //createFacturas.createData;
        res.send("OK");
    },
    insertFactura: function (req, res) {
        return res.status(200).render(
            'insert',
            {
                title: 'Inserción de Facturas',
                page: 'insercion'
            }
        );
    },
    insertManyView: async function (req, res) {
        res.status(200).render('insertMany', {
            title: 'Inserción múltiple de facturas',
            page: 'insertMany'
        });
    }, statistics: function (req, res) {
        res.status(200).render('showStatistics', {
            title: 'Estadísticas por Sector',
            page: 'estadisticas'
        });
    },
    showStatisticsMenor: function (req, res) {
        res.status(200).render('estadisticasPorMenor', {
            title: 'Estadísticas por Sector',
            page: 'estadisticas'
        });
    },
    showStatistics: async function (req, res) {
        let sector = req.query.sector;
        let nif = req.query.nif;

        switch (sector) {
            case "hosteleria":

                res.status(200).send(await estadisticasHosteleria(nif));
                //res.status(200).send(pruebasEstadisticasHosteleria());
                break;
            case "maquinaria":
                res.status(200).send(estadisticasMaquinaria(nif));
                break;
            default:
                res.status(400).send("Incorrent Query");
                break;
        }
    },
    insertMany: async function (req, res) {

        var num_agrupadas = req.query.num;

        const client = new cassandra.Client({
            contactPoints: ['127.0.0.1'],
            keyspace: 'ticketbai',
            localDataCenter: 'datacenter1'
        });
        var tbai_list = [];
        var agrupacion = fs.readFileSync(FACTURAS_AGRUPADAS_PATH + "grupo_" + num_agrupadas + "_1.xml").toString();
        tbai_list.push(DATA.getIdentTBAI(agrupacion));

        var nif = DATA.getNif(agrupacion);
        var fecha_inicio_agrupacion = DATA.getFechaExp(agrupacion);
        var fecha_fin_agrupacion = moment(fecha_inicio_agrupacion, "DD-MM-YYYY").add(7, "d").format("DD-MM-YYYY");

        for (var i = 2; i < num_agrupadas; i++) {
            let factura = fs.readFileSync(FACTURAS_AGRUPADAS_PATH + "grupo_" + num_agrupadas + "_" + i + ".xml").toString();
            agrupacion += factura;
            tbai_list.push(DATA.getIdentTBAI(factura));
        }

        var compresion_start = performance.now();
        var agrupacion_compress = await compressData(agrupacion);
        var compresion_fin = performance.now();

        const insert_query = "insert into facturas_agrupadas (nif, fecha_inicio, agrupacion, fecha_fin, tbai_id_list) values (?,?,?,?,?)";
        const params = [
            nif,
            moment(fecha_inicio_agrupacion, "DD-MM-YYYY").format("YYYY-MM-DD"),
            agrupacion_compress,
            moment(fecha_fin_agrupacion, "DD-MM-YYYY").format("YYYY-MM-DD"),
            tbai_list
        ];
        console.log("INSERT");
        var insertar_cassandra_start = performance.now();
        try{
            await client.execute(insert_query, params, { prepare: true });
        }catch(err){
            console.log(err);
        }
        
        var insert_cassandra_fin = performance.now();
        console.log("INSERT MONGO");
        /**Comprobación de particiones para insertar en Mongo */

        var data_to_insert = {};
        data_to_insert.nif = nif;
        data_to_insert.fechaInicio = moment(fecha_inicio_agrupacion, "DD-MM-YYYY").toDate();
        data_to_insert.fechaFin = moment(fecha_fin_agrupacion, "DD-MM-YYYY").toDate();
        data_to_insert.idents = tbai_list;
        data_to_insert.agrupacion = agrupacion_compress;

        let numParticiones = 0;
        //BYTES DE TODO EL DOCUMENTO A INSERTAR EN LA BD (NO PUEDE SUPERAR LOS 16MB)
        let bytes = new TextEncoder().encode(JSON.stringify(data_to_insert)).byteLength;
        //CALCULO EL NUMERO DE PARTICIONES DEL DOCUMENTO
        if (bytes % (15 * MB) == 0) {
            numParticiones = Math.floor(bytes / (15 * MB));
        } else {
            numParticiones = 1 + Math.floor(bytes / (15 * MB));
        }
        var insert_array = [];
        var comprimir_mongo = [];
        if (numParticiones == 1) {
            insert_array.push(data_to_insert);
            comprimir_mongo.push(compresion_fin - compresion_start);
        } else {
            for (var j = 0; j < numParticiones; j++) {
                var agrupacion_mongo = "";
                var tbai_part_list = [];
                for (var k = Math.round(((j * num_agrupadas) / numParticiones)) + 1; k <= Math.round((j + 1) * num_agrupadas) / numParticiones; k++) {
                    let factura = fs.readFileSync(FACTURAS_AGRUPADAS_PATH + "grupo_" + num_agrupadas + "_" + k + ".xml").toString();
                    agrupacion_mongo += factura;
                    tbai_part_list.push(DATA.getIdentTBAI(factura));
                }
                var compress_mongo_start = performance.now();
                let agrupacion_mongo_compress = await compressData(agrupacion_mongo);
                var compress_mongo_fin = performance.now();
                let new_data_to_insert = {};
                new_data_to_insert.nif = nif;
                new_data_to_insert.fechaInicio = moment(fecha_inicio_agrupacion, "DD-MM-YYYY").toDate();
                new_data_to_insert.fechaFin = moment(fecha_fin_agrupacion, "DD-MM-YYYY").toDate();
                new_data_to_insert.idents = tbai_part_list;
                new_data_to_insert.agrupacion = agrupacion_mongo_compress;

                insert_array.push(new_data_to_insert);
                comprimir_mongo.push(compress_mongo_fin - compress_mongo_start);
            }
        }
        console.log("FIN");
        var insert_mongo_start = performance.now();
        await insert_agrupadas_mongo(insert_array);
        var insert_mongo_fin = performance.now();

        res.status(200).send({
            tbai_id: tbai_list[tbai_list.length - 1],
            stats: {
                insert_cassandra: insert_cassandra_fin - insertar_cassandra_start,
                insert_mongo: insert_mongo_fin - insert_mongo_start,
                comprimir_total_cassandra: compresion_fin - compresion_start,
                comprimir_mongo: comprimir_mongo
            }
        });


    },
    tctest: async function (req, res) {
        var num = req.query.num;
        //console.log(num);
        if (num == null || num == 0 || num > 5000) {
            num = 50;
        }

        var gzip_time_list = [];
        var brotli_time_list = [];
        var lzma_time_list = [];
        var lzma2_time_list = []
        var lzss_time_list = [];

        var gzip_decompress_time_list = [];
        var brotli_decompress_time_list = [];
        var lzma_decompress_time_list = [];
        var lzma2_decompress_time_list = [];
        var lzss_decompress_time_list = [];

        var gzip_ratio_list = [];
        var brotli_ratio_list = [];
        var lzma_ratio_list = [];
        var lzma2_ratio_list = [];
        var lzss_ratio_list = [];


        var labels = [];
        for (var i = 1; i <= num; i++) {
            let factura = fs.readFileSync('./facturas/factura_' + i + '.xml').toString();
            let bytes_start = new TextEncoder().encode(factura).byteLength;

            //console.log("Inicio GZIP");
            //GZIP
            var gzip_compresion_start = performance.now();
            let compress_gzip = await zlib.gzipSync(factura, { level: 1 });
            var gzip_compresion_fin = performance.now();
            var gzip_decompresion_start = performance.now();
            let decompress_gzip = await zlib.gunzipSync(compress_gzip);
            var gzip_decompresion_fin = performance.now();
            //console.log("Inicio Brotli");
            //BROTLI
            var brotli_compresion_start = performance.now();
            let compress_broli = await zlib.brotliCompressSync(factura);
            var brotli_compresion_fin = performance.now();
            var brotli_decompresion_start = performance.now();
            let decompress_broli = await zlib.brotliDecompressSync(compress_broli);
            var brotli_decompresion_fin = performance.now();

            
            //console.log("Inicio LZMA");
            //LZMA
            var lzma_compresion_start = performance.now();
            let compress_lzma = await compress_lzma_file(factura);
            var lzma_compresion_fin = performance.now();
            var lzma_decompresion_start = performance.now();
            let decompress_lzma = await decompress_lzma_file(compress_lzma);
            var lzma_decompresion_fin = performance.now();

            //console.log("Inicio LZMA2");
            //LZMA2
            var lzma2_compresion_start = performance.now();
            let compress_lzma2 = await compress_lzma2_file(factura);
            var lzma2_compresion_fin = performance.now();
            var lzma2_decompresion_start = performance.now();
            let decompress_lzma2 = await decompress_lzma2_file(compress_lzma2);
            var lzma2_decompresion_fin = performance.now();

            //console.log("Inicio LZSS");
            //LZSS
            var lzss_compresion_start = performance.now();
            var compress_lzss = await lzss.compress(factura);
            var lzss_compresion_fin = performance.now();
            var lzss_decompresion_start = performance.now();
            var decompress_lzss = await lzss.decompress(compress_lzss);
            var lzss_decompresion_fin = performance.now();


            gzip_time_list.push(gzip_compresion_fin - gzip_compresion_start);
            brotli_time_list.push(brotli_compresion_fin - brotli_compresion_start);
            lzma_time_list.push(lzma_compresion_fin - lzma_compresion_start);
            lzma2_time_list.push(lzma2_compresion_fin - lzma2_compresion_start);
            lzss_time_list.push(lzss_compresion_fin - lzss_compresion_start);

            gzip_decompress_time_list.push(gzip_decompresion_fin - gzip_decompresion_start);
            brotli_decompress_time_list.push(brotli_decompresion_fin - brotli_decompresion_start);
            lzma_decompress_time_list.push(lzma_decompresion_fin - lzma_decompresion_start);
            lzma2_decompress_time_list.push(lzma2_decompresion_fin - lzma2_decompresion_start);
            lzss_decompress_time_list.push(lzss_decompresion_fin - lzss_decompresion_start);

            gzip_ratio_list.push(1 - (compress_gzip.byteLength / bytes_start));
            brotli_ratio_list.push(1 - (compress_broli.byteLength / bytes_start));
            lzma_ratio_list.push(1 - (Buffer.byteLength(compress_lzma) / bytes_start));
            lzma2_ratio_list.push(1 - (Buffer.byteLength(compress_lzma2) / bytes_start));
            lzss_ratio_list.push(1 - (Buffer.byteLength(compress_lzss) / bytes_start));

            labels.push(i);
        }//End For


        let script_time = `<script>
        var ctx = document.getElementById("compress_time_chart");
        const labels_time = ["${labels.join('\","')}"];
            const data_time = {
            labels: labels_time,
            datasets : [{
                label: "GZip",
                data: [ ${gzip_time_list.toString()}],
                fill:false,
                lineTension:0.3,
                backgroundColor: "rgb(75, 192, 192, 0.05)",
                pointRadius: 3,
                pointBackgroundColor: "rgb(75, 192, 192, 1)",
                pointBorderColor : "rgb(75, 192, 192, 1)",
                pointHoverRadius: 3,
                pointHoverBackgroundColor : "rgb(75, 192, 192, 1)",
                pointHoverBorderColor: "rgb(75, 192, 192, 1)",
                pointHitRadius: 10,
                pointBorderWidth: 2,
                borderColor: "rgb(75, 192, 192)"
            },
            {
                label: "Brotli",
                data: [${brotli_time_list.toString()}],
                fill:false,
                lineTension:0.3,
                backgroundColor: "rgb(255, 0, 0, 0.05)",
                pointRadius: 3,
                pointBackgroundColor: "rgb(255, 0, 0, 1)",
                pointBorderColor : "rgb(255, 0, 0, 1)",
                pointHoverRadius: 3,
                pointHoverBackgroundColor : "rgb(255, 0, 0, 1)",
                pointHoverBorderColor: "rgb(255, 0, 0, 1)",
                pointHitRadius: 10,
                pointBorderWidth: 2,
                borderColor: "rgb(255, 0, 0)"
            },
            {
                label: "LZMA",
                data: [${lzma_time_list.toString()}],
                fill:false,
                lineTension:0.3,
                backgroundColor: "rgb(0, 255, 0, 0.05)",
                pointRadius: 3,
                pointBackgroundColor: "rgb(0, 255, 0, 1)",
                pointBorderColor : "rgb(0, 255, 0, 1)",
                pointHoverRadius: 3,
                pointHoverBackgroundColor : "rgb(0, 255, 0, 1)",
                pointHoverBorderColor: "rgb(0, 255, 0, 1)",
                pointHitRadius: 10,
                pointBorderWidth: 2,
                borderColor: "rgb(0, 255, 0)"
            },
            {
                label: "LZMA2",
                data: [${lzma2_time_list.toString()}],
                fill:false,
                lineTension:0.3,
                backgroundColor: "rgb(0, 64, 162, 0.05)",
                pointRadius: 3,
                pointBackgroundColor: "rgb(0, 64, 162, 1)",
                pointBorderColor : "rgb(0, 64, 162, 1)",
                pointHoverRadius: 3,
                pointHoverBackgroundColor : "rgb(0, 64, 162, 1)",
                pointHoverBorderColor: "rgb(0, 64, 162, 1)",
                pointHitRadius: 10,
                pointBorderWidth: 2,
                borderColor: "rgb(0, 64, 162)"
            },
            {
                label: "LZSS",
                data: [${lzss_time_list.toString()}],
                fill:false,
                lineTension:0.3,
                backgroundColor: "rgb(255, 255, 0, 0.05)",
                pointRadius: 3,
                pointBackgroundColor: "rgb(255, 255, 0, 1)",
                pointBorderColor : "rgb(255, 255, 0, 1)",
                pointHoverRadius: 3,
                pointHoverBackgroundColor : "rgb(255, 255, 0, 1)",
                pointHoverBorderColor: "rgb(255, 255, 0, 1)",
                pointHitRadius: 10,
                pointBorderWidth: 2,
                borderColor: "rgb(255, 255, 0)"
            }
        ]};

            const config_time = {
            type: "line",
            data: data_time,
            options: {
                responsive:true,
                maintainAspectRatio: false,
                layout: {
                  padding: {
                    left: 10,
                    right: 25,
                    top: 25,
                    bottom: 0
                  }
                },
                scales: {
                  xAxes: [{
                    time: {
                      unit: 'number'
                    },
                    gridLines: {
                      display: false,
                      drawBorder: false
                    },
                    ticks: {
                      maxTicksLimit: 6
                    }
                  }],
                  yAxes: [{
                    ticks: {
                        min:0,
                        max: 100,
                        stepSize: 20,
                        callback: function(value, index, values) {
                            return number_format(value) + 'ms';
                      }
                    },
                    gridLines: {
                      color: "rgb(234, 236, 244)",
                      zeroLineColor: "rgb(234, 236, 244)",
                      drawBorder: false,
                      borderDash: [2],
                      zeroLineBorderDash: [2]
                    }
                  }],
                },
                legend: {
                  display: false
                },
                tooltips: {
                  backgroundColor: "rgb(255,255,255)",
                  bodyFontColor: "#858796",
                  titleMarginBottom: 10,
                  titleFontColor: '#6e707e',
                  titleFontSize: 14,
                  borderColor: '#dddfeb',
                  borderWidth: 1,
                  xPadding: 15,
                  yPadding: 5,
                  displayColors: false,
                  intersect: false,
                  mode: 'index',
                  caretPadding: 5,
                  callbacks: {
                    label: function(tooltipItem, chart) {
                      var datasetLabel = chart.datasets[tooltipItem.datasetIndex].label || '';
                      return datasetLabel + ': ' + number_format(tooltipItem.yLabel) + 'ms';
                    }
                  }
                }
              }
            };
            var chart = new Chart(ctx, config_time);</script>`;

        let script_decom = `<script>
        var ctx_decom = document.getElementById("decompress_time_chart");
        const decom_labels_time = ["${labels.join('\","')}"];
        const decom_data_time = {
        labels: decom_labels_time,
        datasets : [{
            label: "GZip",
            data: [${gzip_decompress_time_list.toString()}],
            fill:false,
            lineTension:0.3,
            backgroundColor: "rgb(75, 192, 192, 0.05)",
            pointRadius: 3,
            pointBackgroundColor: "rgb(75, 192, 192, 1)",
            pointBorderColor : "rgb(75, 192, 192, 1)",
            pointHoverRadius: 3,
            pointHoverBackgroundColor : "rgb(75, 192, 192, 1)",
            pointHoverBorderColor: "rgb(75, 192, 192, 1)",
            pointHitRadius: 10,
            pointBorderWidth: 2,
            borderColor: "rgb(75, 192, 192)"
        },
        {
            label: "Brotli",
            data: [${brotli_decompress_time_list.toString()}],
            fill:false,
            lineTension:0.3,
            backgroundColor: "rgb(255, 0, 0, 0.05)",
            pointRadius: 3,
            pointBackgroundColor: "rgb(255, 0, 0, 1)",
            pointBorderColor : "rgb(255, 0, 0, 1)",
            pointHoverRadius: 3,
            pointHoverBackgroundColor : "rgb(255, 2505, 0, 1)",
            pointHoverBorderColor: "rgb(255, 0, 0, 1)",
            pointHitRadius: 10,
            pointBorderWidth: 2,
            borderColor: "rgb(255, 0, 0)"
        },
        {
            label: "LZMA",
            data: [${lzma_decompress_time_list.toString()}],
            fill:false,
            lineTension:0.3,
            backgroundColor: "rgb(0, 255, 0, 0.05)",
            pointRadius: 3,
            pointBackgroundColor: "rgb(0, 255, 0, 1)",
            pointBorderColor : "rgb(0, 255, 0, 1)",
            pointHoverRadius: 3,
            pointHoverBackgroundColor : "rgb(0, 255, 0, 1)",
            pointHoverBorderColor: "rgb(0, 255, 0, 1)",
            pointHitRadius: 10,
            pointBorderWidth: 2,
            borderColor: "rgb(0, 255, 0)"
        },
        {
            label: "LZMA2",
            data: [${lzma2_decompress_time_list.toString()}],
            fill:false,
            lineTension:0.3,
            backgroundColor: "rgb(0, 64, 162, 0.05)",
            pointRadius: 3,
            pointBackgroundColor: "rgb(0, 64, 162, 1)",
            pointBorderColor : "rgb(0, 64, 162, 1)",
            pointHoverRadius: 3,
            pointHoverBackgroundColor : "rgb(0, 64, 162, 1)",
            pointHoverBorderColor: "rgb(0, 64, 162, 1)",
            pointHitRadius: 10,
            pointBorderWidth: 2,
            borderColor: "rgb(0, 64, 162)"
        },
        {
            label: "LZSS",
            data: [${lzss_decompress_time_list.toString()}],
           fill:false,
            lineTension:0.3,
            backgroundColor: "rgb(255, 255, 0, 0.05)",
            pointRadius: 3,
            pointBackgroundColor: "rgb(255, 255, 0, 1)",
            pointBorderColor : "rgb(255, 255, 0, 1)",
            pointHoverRadius: 3,
            pointHoverBackgroundColor : "rgb(255, 255, 0, 1)",
            pointHoverBorderColor: "rgb(255, 255, 0, 1)",
            pointHitRadius: 10,
            pointBorderWidth: 2,
            borderColor: "rgb(255, 255, 0)"
        }
        ]};

        const decom_config_time = {
        type: "line",
        data: decom_data_time,
        options: {
            maintainAspectRatio: false,
            layout: {
              padding: {
                left: 10,
                right: 25,
                top: 25,
                bottom: 0
              }
            },
            scales: {
              xAxes: [{
                time: {
                  unit: 'number'
                },
                gridLines: {
                  display: false,
                  drawBorder: false
                },
                ticks: {
                  maxTicksLimit: 7
                }
              }],
              yAxes: [{
                ticks: {
                  maxTicksLimit: 5,
                  padding: 10,
                  // Include a dollar sign in the ticks
                  callback: function(value, index, values) {
                    return number_format(value) + 'ms';
                  }
                },
                gridLines: {
                  color: "rgb(234, 236, 244)",
                  zeroLineColor: "rgb(234, 236, 244)",
                  drawBorder: false,
                  borderDash: [2],
                  zeroLineBorderDash: [2]
                }
              }],
            },
            legend: {
              display: false
            },
            tooltips: {
              backgroundColor: "rgb(255,255,255)",
              bodyFontColor: "#858796",
              titleMarginBottom: 10,
              titleFontColor: '#6e707e',
              titleFontSize: 14,
              borderColor: '#dddfeb',
              borderWidth: 1,
              xPadding: 15,
              yPadding: 15,
              displayColors: false,
              intersect: false,
              mode: 'index',
              caretPadding: 10,
              callbacks: {
                label: function(tooltipItem, chart) {
                  var datasetLabel = chart.datasets[tooltipItem.datasetIndex].label || '';
                  return datasetLabel + ': ' + number_format(tooltipItem.yLabel) + 'ms';
                }
              }
            }
          }
        };
        var decom_chart = new Chart(ctx_decom, decom_config_time);</script>`;

        let script_ratio = `
        <script>
            var ctx = document.getElementById("compress_ratio_chart");
            const labels_ratio = ["${labels.join('\","')}"];
            const data_ratio = {
                labels: labels_ratio,
                datasets : [
                    {
                        label: "GZip",
                        data: [${gzip_ratio_list.toString()}],
                       fill:false,
                        lineTension:0.3,
                        backgroundColor: "rgb(75, 192, 192, 0.05)",
                        pointRadius: 3,
                        pointBackgroundColor: "rgb(75, 192, 192, 1)",
                        pointBorderColor : "rgb(75, 192, 192, 1)",
                        pointHoverRadius: 3,
                        pointHoverBackgroundColor : "rgb(75, 192, 192, 1)",
                        pointHoverBorderColor: "rgb(75, 192, 192, 1)",
                        pointHitRadius: 10,
                        pointBorderWidth: 2,
                        borderColor: "rgb(75, 192, 192)"
                    },
                    {
                        label: "Brotli",
                        data: [${brotli_ratio_list.toString()}],
                       fill:false,
                lineTension:0.3,
                backgroundColor: "rgb(255, 0, 0, 0.05)",
                pointRadius: 3,
                pointBackgroundColor: "rgb(255, 0, 0, 1)",
                pointBorderColor : "rgb(255, 0, 0, 1)",
                pointHoverRadius: 3,
                pointHoverBackgroundColor : "rgb(255, 0, 0, 1)",
                pointHoverBorderColor: "rgb(255, 0, 0, 1)",
                pointHitRadius: 10,
                pointBorderWidth: 2,
                        borderColor: "rgb(255, 0, 0)"
                    },
                    {
                        label: "LZMA",
                        data: [${lzma_ratio_list.toString()}],
                       fill:false,
                lineTension:0.3,
                backgroundColor: "rgb(0, 255, 0, 0.05)",
                pointRadius: 3,
                pointBackgroundColor: "rgb(0, 255, 0, 1)",
                pointBorderColor : "rgb(0, 255, 0, 1)",
                pointHoverRadius: 3,
                pointHoverBackgroundColor : "rgb(0, 255, 0, 1)",
                pointHoverBorderColor: "rgb(0, 255, 0, 1)",
                pointHitRadius: 10,
                pointBorderWidth: 2,
                        borderColor: "rgb(0, 255, 0)"
                    },
                    {
                        label: "LZMA2",
                        data: [${lzma2_ratio_list.toString()}],
                       fill:false,
                lineTension:0.3,
                backgroundColor: "rgb(0, 64, 162, 0.05)",
                pointRadius: 3,
                pointBackgroundColor: "rgb(0, 64, 162, 1)",
                pointBorderColor : "rgb(0, 64, 162, 1)",
                pointHoverRadius: 3,
                pointHoverBackgroundColor : "rgb(0, 64, 162, 1)",
                pointHoverBorderColor: "rgb(0, 64, 162, 1)",
                pointHitRadius: 10,
                pointBorderWidth: 2,
                        borderColor: "rgb(0, 64, 162)"
                    },
                    {
                        label: "LZSS",
                        data: [${lzss_ratio_list.toString()}],
                       fill:false,
                        lineTension:0.3,
                        backgroundColor: "rgb(255, 255, 0, 0.05)",
                        pointRadius: 3,
                        pointBackgroundColor: "rgb(255, 255, 0, 1)",
                        pointBorderColor : "rgb(255, 255, 0, 1)",
                        pointHoverRadius: 3,
                        pointHoverBackgroundColor : "rgb(255, 255, 0, 1)",
                        pointHoverBorderColor: "rgb(255, 255, 0, 1)",
                        pointHitRadius: 10,
                        pointBorderWidth: 2,
                        borderColor: "rgb(255, 255, 0)"
                    }
                ]
            };

            const config_ratio = {
            type: "line",
            data: data_ratio,
            options: {
                maintainAspectRatio: false,
                layout: {
                  padding: {
                    left: 10,
                    right: 25,
                    top: 25,
                    bottom: 0
                  }
                },
                scales: {
                  xAxes: [{
                    time: {
                      unit: 'number'
                    },
                    gridLines: {
                      display: false,
                      drawBorder: false
                    },
                    ticks: {
                      maxTicksLimit: 7
                    }
                  }],
                  yAxes: [{
                    ticks: {
                      maxTicksLimit: 5,
                      padding: 10,
                      // Include a dollar sign in the ticks
                      callback: function(value, index, values) {
                        return number_format(value) + '%';
                      }
                    },
                    gridLines: {
                      color: "rgb(234, 236, 244)",
                      zeroLineColor: "rgb(234, 236, 244)",
                      drawBorder: false,
                      borderDash: [2],
                      zeroLineBorderDash: [2]
                    }
                  }],
                },
                legend: {
                  display: false
                },
                tooltips: {
                  backgroundColor: "rgb(255,255,255)",
                  bodyFontColor: "#858796",
                  titleMarginBottom: 10,
                  titleFontColor: '#6e707e',
                  titleFontSize: 14,
                  borderColor: '#dddfeb',
                  borderWidth: 1,
                  xPadding: 15,
                  yPadding: 15,
                  displayColors: false,
                  intersect: false,
                  mode: 'index',
                  caretPadding: 10,
                  callbacks: {
                    label: function(tooltipItem, chart) {
                      var datasetLabel = chart.datasets[tooltipItem.datasetIndex].label || '';
                      return datasetLabel + ': ' + number_format(tooltipItem.yLabel) + '%';
                    }
                  }
                }
              }
            };
            var chart = new Chart(ctx, config_ratio);</script>`;


        res.status(200).render('tctest', {
            title: 'Inserción de Facturas',
            page: 'Pruebas Técnicas de Compresión',
            script_time: script_time,
            script_ratio: script_ratio,
            script_decom: script_decom
        });

    },
    getFactura: async function (req, res) {

        console.log("getFactura");
        res.status(200).render('getfactura', {
            title: 'Búsqueda de Facturas',
            page: 'getFactura'
        });
    },
    gr: async function (req, res) {
        const regex = /^TBAI-[0-9]{8}[A-Z]-[0-9]{6}-.{13}-[0-9]{3}$/;;
        //document.getElementById('loading_gif').style.display = "block";
        var tbai_id = String(req.query.id);
        if (!tbai_id.match(regex)) {
            res.status(200).render('gr', {
                title: 'Búsqueda de Facturas',
                page: 'getFactura',
                error: '<script>document.getElementById("data").style.display = "none";</script><script>document.getElementById("tbai_error").style.display = "block";</script>',
                data: ""
            });
        } else {

            const client = new cassandra.Client({
                contactPoints: ['127.0.0.1'],
                keyspace: 'ticketbai',
                localDataCenter: 'datacenter1'
            });

            var result_mongo = await findByTBAI(tbai_id);
            var result_cassandra = await findByIdCassandra(tbai_id, client);
            //let result_cassandra = {
            //    stats: {}
            //};



            let script = `
            <script>
                $("#agrupadas-chart").hide();
                if(Chart.getChart("get_factura_chart") != null){
                    Chart.getChart("get_factura_chart").destroy();
                }
                if(Chart.getChart("descompresion-chart") != null){
                    Chart.getChart("descompresion-chart").destroy();
                }
                if(Chart.getChart("recuperacion-agrupacion-chart") != null){
                    Chart.getChart("recuperacion-agrupacion-chart").destroy();
                }
                    var ctx = document.getElementById("get_factura_chart");
                    const labels = ["MongoDB", "Cassandra"];
                    const data = {
                    labels: labels,
                    datasets:[{
                        label: "Tiempo de Obtención de Datos",                      
                        backgroundColor: ["rgba(255, 99, 132, 1)", "rgba(54, 162, 235, 1)"],
                        borderColor: ["rgb(255, 99, 132)","rgb(54, 162, 235)"],
                        borderWidth: 1,
                        data: [${(result_mongo.stats.busqueda_datos)}, ${(result_cassandra.stats.busqueda_datos)}],
                    }]
                };

                const config = {
                    type: "bar",
                    data: data,
                    options : {
                        maintainAspectRatio: false,
                        layout: {
                        padding: {
                            left: 10,
                            right: 25,
                            top: 25,
                            bottom: 0
                        }
                        },
                        scales: {
                        xAxes: [{
                            time: {
                            unit: 'month'
                            },
                            gridLines: {
                            display: false,
                            drawBorder: false
                            },
                            ticks: {
                            maxTicksLimit: 2
                            },
                            maxBarThickness: 10,
                        }],
                        yAxes: [{
                            ticks: {
                            min: 0,
                            max: 2000,
                            maxTicksLimit: 5,
                            padding: 10,
                            // Include a dollar sign in the ticks
                            callback: function(value, index, values) {
                                return '$' + number_format(value);
                            }
                            },
                            gridLines: {
                            color: "rgb(234, 236, 244)",
                            zeroLineColor: "rgb(234, 236, 244)",
                            drawBorder: false,
                            borderDash: [2],
                            zeroLineBorderDash: [2]
                            }
                        }],
                        },
                        legend: {
                        display: false
                        },
                        tooltips: {
                        titleMarginBottom: 10,
                        titleFontColor: '#6e707e',
                        titleFontSize: 14,
                        backgroundColor: "rgb(255,255,255)",
                        bodyFontColor: "#858796",
                        borderColor: '#dddfeb',
                        borderWidth: 1,
                        xPadding: 15,
                        yPadding: 15,
                        displayColors: false,
                        caretPadding: 10,
                        callbacks: {
                            label: function(tooltipItem, chart) {
                            var datasetLabel = chart.datasets[tooltipItem.datasetIndex].label || '';
                            return datasetLabel + ': $' + number_format(tooltipItem.yLabel);
                            }
                        }
                        }
                    }
                };
                var chart = new Chart(ctx, config);</script>
            `;
            var script_decom = "";
            var script_busqueda_fact = "";
            if (result_mongo.agrupada) {
                script_decom = `<script>
                $("#agrupadas-chart").show();
                var ctx_decom = document.getElementById("descompresion-chart");
                const labels_decom = ["MongoDB", "Cassandra"];
                const data_decom = {
                labels: labels_decom,
                datasets:[{
                label: "Tiempo de descompresión (milisegundos)",
                data: [ ${(result_mongo.stats.descompresion)}, ${(result_cassandra.stats.descompresion)}],
                backgroundColor: ["rgba(255, 99, 132, 0.2)", "rgba(54, 162, 235, 0.2)"],
                borderColor: ["rgb(255, 99, 132)","rgb(54, 162, 235)"],
                borderWidth: 1
                }]
                };

                const config_decom = {
                type: "bar",
                data: data_decom,
                maintainAspectRatio: false,
                layout: {
                    padding: {
                        left: 10,
                        right: 25,
                        top: 25,
                        bottom: 0
                    }
                    },
                    scales: {
                    xAxes: [{
                        time: {
                        unit: 'month'
                        },
                        gridLines: {
                        display: false,
                        drawBorder: false
                        },
                        ticks: {
                        maxTicksLimit: 2
                        },
                        maxBarThickness: 10,
                    }],
                    yAxes: [{
                        ticks: {
                        min: 0,
                        max: 2000,
                        maxTicksLimit: 5,
                        padding: 10,
                        // Include a dollar sign in the ticks
                        callback: function(value, index, values) {
                            return '$' + number_format(value);
                        }
                        },
                        gridLines: {
                        color: "rgb(234, 236, 244)",
                        zeroLineColor: "rgb(234, 236, 244)",
                        drawBorder: false,
                        borderDash: [2],
                        zeroLineBorderDash: [2]
                        }
                    }],
                    },
                    legend: {
                    display: false
                    },
                    tooltips: {
                    titleMarginBottom: 10,
                    titleFontColor: '#6e707e',
                    titleFontSize: 14,
                    backgroundColor: "rgb(255,255,255)",
                    bodyFontColor: "#858796",
                    borderColor: '#dddfeb',
                    borderWidth: 1,
                    xPadding: 15,
                    yPadding: 15,
                    displayColors: false,
                    caretPadding: 10,
                    callbacks: {
                        label: function(tooltipItem, chart) {
                        var datasetLabel = chart.datasets[tooltipItem.datasetIndex].label || '';
                        return datasetLabel + ': $' + number_format(tooltipItem.yLabel);
                        }
                    }
                    }
                };
                var chart = new Chart(ctx_decom, config_decom);</script>`;

            script_busqueda_fact = `<script>
                var ctx_busqueda_fact = document.getElementById("recuperacion-agrupacion-chart");
                const labels_busqueda_fact = ["MongoDB", "Cassandra"];
                const data_busqueda_fact = {
                labels: labels_busqueda_fact,
                datasets:[{
                label: "Tiempo de Búsqueda en la Agrupación (milisegundos)",
                data: [ ${(result_mongo.stats.busqueda_factura)}, ${(result_cassandra.stats.busqueda_factura)}],
                backgroundColor: ["rgba(255, 99, 132, 0.2)", "rgba(54, 162, 235, 0.2)"],
                borderColor: ["rgb(255, 99, 132)","rgb(54, 162, 235)"],
                borderWidth: 1
                }]
                };

                const config_busqueda_fact = {
                type: "bar",
                data: data_busqueda_fact,
                maintainAspectRatio: false,
                layout: {
                    padding: {
                        left: 10,
                        right: 25,
                        top: 25,
                        bottom: 0
                    }
                    },
                    scales: {
                    xAxes: [{
                        time: {
                        unit: 'month'
                        },
                        gridLines: {
                        display: false,
                        drawBorder: false
                        },
                        ticks: {
                        maxTicksLimit: 2
                        },
                        maxBarThickness: 10,
                    }],
                    yAxes: [{
                        ticks: {
                        min: 0,
                        max: 2000,
                        maxTicksLimit: 5,
                        padding: 10,
                        // Include a dollar sign in the ticks
                        callback: function(value, index, values) {
                            return '$' + number_format(value);
                        }
                        },
                        gridLines: {
                        color: "rgb(234, 236, 244)",
                        zeroLineColor: "rgb(234, 236, 244)",
                        drawBorder: false,
                        borderDash: [2],
                        zeroLineBorderDash: [2]
                        }
                    }],
                    },
                    legend: {
                    display: false
                    },
                    tooltips: {
                    titleMarginBottom: 10,
                    titleFontColor: '#6e707e',
                    titleFontSize: 14,
                    backgroundColor: "rgb(255,255,255)",
                    bodyFontColor: "#858796",
                    borderColor: '#dddfeb',
                    borderWidth: 1,
                    xPadding: 15,
                    yPadding: 15,
                    displayColors: false,
                    caretPadding: 10,
                    callbacks: {
                        label: function(tooltipItem, chart) {
                        var datasetLabel = chart.datasets[tooltipItem.datasetIndex].label || '';
                        return datasetLabel + ': $' + number_format(tooltipItem.yLabel);
                        }
                    }
                    }
                };
                var chart = new Chart(ctx_busqueda_fact, config_busqueda_fact);</script>`;
            }


            res.status(200).render('gr', {
                title: 'Búsqueda de Facturas',
                page: 'getFactura',
                error: '<script>document.getElementById("data").style.display = "block";</script><script>document.getElementById("tbai_error").style.display = "none";</script>',
                tbai_id: result_mongo.data.tbai_id,
                nif_emisor: result_mongo.data.nif_emisor,
                serie_factura: result_mongo.data.serie_factura,
                num_factura: result_mongo.data.num_factura,
                importe_factura: result_mongo.data.importe_factura + " €",
                fecha_exp: moment(result_mongo.data.fecha_exp, "DD-MM-YYYY").format("YYYY/MM/DD"),
                script: script,
                script_decom: script_decom,
                script_busqueda_fact: script_busqueda_fact
            });
        }



    },
    insercionFacturas: async function (req, res) {

        if (req.files) {
            var filePath = req.files.file.path;
            var fileExt = filePath.split('\\')[1].split(".")[1];

            if (fileExt == "xml") {//Simplemente lo subo

                const client = new cassandra.Client({
                    contactPoints: ['127.0.0.1'],
                    keyspace: 'ticketbai',
                    localDataCenter: 'datacenter1'
                });

                var factura = fs.readFileSync(filePath).toString();
                //INSERCION en MONGODB
                var compress_gzip = await compressData(factura);
                var insercion_mongo_start = performance.now();
                let json = {};
                json._id = DATA.getIdentTBAI(factura);
                json.nif = DATA.getNif(factura);
                json.fecha = moment(DATA.getFechaExp(factura), "DD-MM-YYYY").toDate();
                //json.HoraExpedicionFactura = moment(DATA.getHoraExpedionFactura(factura), "hh:mm:ss").toDate();
                json.cantidad = DATA.getImporteTotalFactura(factura);
                json.serie = DATA.getSerieFactura(factura);
                json.num_factura = DATA.getNumFactura(factura);
                //json.Descripcion = DATA.getDescripcion(factura);
                //json.FacturaComprimida = compress_gzip.toString("base64");
                json.xml = compress_gzip;
                json.status = 0;
                let resul = await insert_mongo([json]).catch((err) => {
                    //throw err;
                    return err.code;
                }).then((res) => {
                    //console.log(err);
                    if (res) {
                        return res;
                    }
                    return "OK";
                });

                if (resul != "OK") {//Ha ocurrido algun tipo de error
                    fs.unlinkSync(filePath);
                    return res.status(200).send(
                        {
                            title: 'Inserción de Facturas',
                            page: 'insertFacturas',
                            tbai_id: resul
                            //file: compressed
                        }
                    );
                }
                var insercion_mongo_fin = performance.now();

                //Insercion en Cassandra
                var insercion_cassandra_start = performance.now();
                const insertQuery = "insert into facturas (nif, fecha, tbai_id, importe, num_factura, serie, xml) values (?, ?, ?, ?, ?, ?, ?)";
                const params = [
                    DATA.getNif(factura),
                    moment(DATA.getFechaExp(factura), "DD-MM-YYYY").toDate(),
                    DATA.getIdentTBAI(factura),
                    DATA.getImporteTotalFactura(factura),
                    DATA.getNumFactura(factura),
                    DATA.getSerieFactura(factura),
                    compress_gzip
                ];
                await client.execute(insertQuery, params, { prepare: true });
                var insercion_cassandra_fin = performance.now();
                fs.unlinkSync(filePath);
                res.status(200).send({
                    tbai_id: json._id,
                    title: 'Inserción de Facturas',
                    page: 'insertFacturas'
                });


            } else {//Error el formato no es correcto
                res.status(200).send({
                    tbai_id: -2,
                    title: 'Inserción de Facturas',
                    page: 'insertFacturas'
                });
            }

        } else {//error al enviar los archivos
            res.status(200).send(
                {
                    title: 'Inserción de Facturas',
                    page: 'insertFacturas',
                    tbai_id: -3
                }
            );
        }
    }, insertFacturasEstadisticas: async function (req, res) {

        //const DIRECTORY_PATH = "/Users/gorkaalvarez/Desktop/Uni/tbaiData/";
        const DIRECTORY_PATH = "C:\\Users\\877205\\Desktop\\FacturasInsert\\insertData\\";
        //await mongoose.connect(mongoUrl + "/" + dbName).then(() => { console.log("Conexión a MongoDB realizada correctamente") });
        const index = fs.readFileSync(DIRECTORY_PATH + "index.txt").toString().split("\n");
        const client = new cassandra.Client({
            contactPoints: ['127.0.0.1'],
            keyspace: 'ticketbai',
            localDataCenter: 'datacenter1'
        });
         
        for (var i = 0; i < index.length; i++) {
            //let nif = companies_nif_list[i][0];
            let file = index[i].split("/")[5];
            //let file = index[i];
            //console.log(DIRECTORY_PATH+file);
            try {
                var facturas = JSON.parse(fs.readFileSync(DIRECTORY_PATH + file).toString());
                //await insert_mongo(facturas).then(() => {console.log("Guardada --> "+file)}).catch(() => {console.log("Error al guardar --> "+file)});
            } catch (err) {
                console.log("Error al leer la factura --> " + file);
            }

            var array = [];
            console.log(facturas.length);
            for (var j = 0; j < facturas.length; j++) {
                let factura_j = facturas[j];
               /* let data = {};
                data._id = factura_j.id_tbai;
                //data._id = factura_j._id;
                data.nif = factura_j.nif;
                data.fecha = moment(factura_j.fecha).toDate();
                data.cantidad = factura_j.cantidad;
                data.serie = factura_j.serie;
                data.status = factura_j.status;
                data.xml = factura_j.xml;
                array.push(data);
                */
                const insertQuery = "insert into facturas (nif, fecha, tbai_id, importe, num_factura, serie, xml) values (?, ?, ?, ?, ?, ?, ?)";
                const params = [
                    factura_j.nif,
                    factura_j.fecha,
                    factura_j.id_tbai,
                    factura_j.cantidad,
                    "000001",
                    factura_j.serie,
                    factura_j.xml
                ];
                await client.execute(insertQuery, params, { prepare: true }).catch((err) => {
                   throw err;
                });
                
            }
            console.log("Guardada --> " + file);
            //await insert_mongo(array).then(() => { console.log("Guardada --> " + file) }).catch(() => { console.log("Error al guardar --> " + file) });
            



        }

        res.status(200).send("OK");

    },
    agruparMes: async function (req, res) {
        var nif_array = companies_nif_list.slice(0, 763).map(n => n[0]);
        for (var i = 0; i < nif_array.length; i++) {

            for (var t = moment("2021-01-01").toDate(); t < moment("2021-04-01").toDate(); t = moment(t).add(1, "months").toDate()) {
                let t_aux = moment(t);
                let facturas = await Factura.find({
                    nif: nif_array[i],
                    fecha: {
                        $gte: t,
                        $lte: t_aux.add(1, "months").toDate()
                    }
                }, "xml").exec();
                if (facturas != null) {
                    var agrupacion = "";
                    var tbai_array = [];
                    for (var j = 0; j < facturas.length; j++) {
                        let factura = zlib.gunzipSync(Buffer.from(facturas[j].xml, "base64"), GZIP_PARAMS).toString();
                        agrupacion += factura;
                        tbai_array.push(DATA.getIdentTBAI(factura));
                    }

                    var data_to_insert = {};
                    data_to_insert.nif = nif_array[i];
                    data_to_insert.fechaInicio = t;
                    data_to_insert.fechaFin = t_aux.subtract(1, "days").toDate();
                    data_to_insert.idents = tbai_array;
                    data_to_insert.agrupacion = zlib.gzipSync(agrupacion, GZIP_PARAMS).toString("base64");
                    await insert_agrupadas_mongo([data_to_insert]);
                } else {
                    console.log("Error al transformar las facturas del nif --> " + nif_array[i]);
                }
            }
            console.log("Insertado NIF --> " + nif_array[i]);
        }

        res.status(200).send("OK");
    }

};





async function findByIdCassandra(tbai_id, client) {
    return new Promise((resolve) => {
        const query_indiv = "select nif, fecha, tbai_id, importe, num_factura, serie from facturas where nif=? and fecha=? and tbai_id = ?";
        const params_indiv = [
            tbai_id.split("-")[1],
            moment(tbai_id.split("-")[2], "DDMMYY").format("YYYY-MM-DD"),
            tbai_id
        ];

        var busqueda_bd_start = performance.now();
        client.execute(query_indiv, params_indiv, { prepare: true }).then((resul) => {
            if (resul.rowLength < 1) {
                const query_gr = "select fecha_fin, tbai_id_list, agrupacion from facturas_agrupadas where nif=? and fecha_inicio <= ?";
                const params_gr = [
                    tbai_id.split("-")[1],
                    moment(tbai_id.split("-")[2], "DDMMYY").format("YYYY-MM-DD")
                ];
                client.execute(query_gr, params_gr, { prepare: true }).then((res) => {
                    var agrupacion = "";
                    var tbai_list;
                    for (var i = 0; i < res.rowLength; i++) {
                        if (moment(res.rows[i].fecha_fin, "YYYY-MM-DD").toDate() >= moment(tbai_id.split("-")[2], "DDMMYY").toDate()) {
                            agrupacion = res.rows[i].agrupacion;
                            tbai_list = res.rows[i].tbai_id_list;
                            break;
                        }
                    }
                    var busqueda_bd_fin = performance.now();
                    var descompresion_start = performance.now();
                    unCompressData(agrupacion).then((resul) => {
                        var descompresion_fin = performance.now();
                        var busqueda_factura_start = performance.now();

                        var pos = Array.from(tbai_list).indexOf(tbai_id);
                        var facturas_array = resul.split(/(?=\<\?xml version="1\.0" encoding="utf-8"\?\>)/);
                        let data = facturas_array[pos];

                        var busqueda_factura_fin = performance.now();

                        resolve({
                            code: 200,
                            agrupada: true,
                            data: {
                                tbai_id: tbai_id,
                                nif_emisor: DATA.getNif(data),
                                serie_factura: DATA.getSerieFactura(data),
                                num_factura: DATA.getNumFactura(data),
                                importe_factura: DATA.getImporteTotalFactura(data),
                                fecha_exp: DATA.getFechaExp(data)
                            },
                            stats: {
                                busqueda_datos: busqueda_bd_fin - busqueda_bd_start,
                                descompresion: descompresion_fin - descompresion_start,
                                busqueda_factura: busqueda_factura_fin - busqueda_factura_start
                            }
                        });
                    });


                });
            } else {
                var busqueda_bd_fin = performance.now();

                resolve({
                    agrupada: false,
                    data: {
                        tbai_id: tbai_id,
                        nif_emisor: resul.rows[0].nif,
                        serie_factura: resul.rows[0].serie,
                        num_factura: resul.rows[0].num_factura,
                        importe_factura: resul.rows[0].importe,
                        fecha_exp: resul.rows[0].fecha
                    },
                    stats: {
                        busqueda_datos: busqueda_bd_fin - busqueda_bd_start
                    }
                });
            }
        });

    });
}


/**
 * Busca la factura que coincide con el id que se le pasa como parametro. Primero busca en la coleccion Facturas,
 * que es la coleccion que contiene las facturas unitarias. Si no lo encuentra, busca en la coleccion de facturas_agrupadas,
 * y devulve la factura correspondiente.
 * @param {string} tbai_id identificador de la factura
 * @returns La factura cuyo identificador coincide con tbai_id
 */
async function findByTBAI(tbai_id) {
    return new Promise((resolve) => {
        var busqueda_datos_start = performance.now();
        Factura.findById(tbai_id, '_id nif fecha cantidad serie num_factura', (err, factura) => {
            var busqueda_datos_fin = performance.now();
            if (err) resolve({
                code: 500,
                data: "Error al devolver los datos",
                stats: {}
            });
            if (!factura) {//No he encontrado una factura con esa id (Estará comprimida)
                //TBAI-82275936Z-010120-dPmfSHpsGqWpI-120
                var tbai_split = tbai_id.split("-");
                let nif = tbai_split[1];
                let fecha = moment(tbai_split[2], "DDMMYY").toDate();
                busqueda_datos_start = performance.now();
                AgrupacionFactura.find({ nif: nif, fechaInicio: { $lte: fecha }, fechaFin: { $gte: fecha } }, (err, docs) => {
                    var busqueda_datos_fin = performance.now();
                    if (err) resolve({
                        code: 500,
                        data: "Error al devolver los datos",
                        stats: {}
                    });
                    if (!docs) resolve({
                        code: 404,
                        data: "No se ha encontrado la factura con ese identificador",
                        stats: {}
                    });

                    for (var i = 0; i < docs.length; i++) {
                        if (docs[i].idents.includes(tbai_id)) {
                            var pos = Array.from(docs[i].idents).indexOf(tbai_id);
                            var agrupacion = docs[i].agrupacion;
                            var descompresion_start = performance.now();

                            unCompressData(agrupacion).then((resul) => {
                                var descompresion_fin = performance.now();

                                var busqueda_factura_start = performance.now();
                                var facturas_array = resul.split(/(?=\<\?xml version="1\.0" encoding="utf-8"\?\>)/);
                                let data = facturas_array[pos];
                                var busqueda_factura_fin = performance.now();


                                resolve({
                                    code: 200,
                                    agrupada: true,
                                    data: {
                                        tbai_id: tbai_id,
                                        nif_emisor: DATA.getNif(data),
                                        serie_factura: DATA.getSerieFactura(data),
                                        num_factura: DATA.getNumFactura(data),
                                        importe_factura: DATA.getImporteTotalFactura(data),
                                        fecha_exp: DATA.getFechaExp(data)
                                    },
                                    stats: {
                                        busqueda_datos: busqueda_datos_fin - busqueda_datos_start,
                                        descompresion: descompresion_fin - descompresion_start,
                                        busqueda_factura: busqueda_factura_fin - busqueda_factura_start
                                    }
                                });
                            });

                        }
                    }
                });
            } else {
                resolve({
                    code: 200,
                    agrupada: false,
                    data: {
                        tbai_id: tbai_id,
                        nif_emisor: factura.nif,
                        serie_factura: factura.serie,
                        num_factura: factura.num_factura,
                        importe_factura: factura.cantidad,
                        fecha_exp: factura.fecha
                    },
                    stats: {
                        busqueda_datos: busqueda_datos_fin - busqueda_datos_start
                    }
                });
            }
        });
    });

}

function executeQuery(query) {
    return new Promise((resolve) => {
        Factura.find(query, (err, result) => {
            if (!err) resolve(result);
        });
    });
}


function createEstadisticaDiaria(nif) {
    return new Promise((resolve, reject) => {
        console.log("Estadistica diaria");


        try {
            var start = performance.now();
            //console.log("Dentro del try");
            if (fs.existsSync("./estadisticas/2021-03-19_2021-03-19_global.txt")) {//Si existe la de un nif entonces existe la global
                //console.log("Existe fichero?");
                var global_dia_estadistica = fs.readFileSync("./estadisticas/2021-03-19_2021-03-19_global.txt").toString().split("\n");
                if (global_dia_estadistica.map(l => l.split(" // ")[0]).filter(n => n == `dia_${nif}`).length == 0) {//No existe la estadistica sobre ese nif
                    //console.log("No existe estadistica sobre el nif");
                    //dia_28693295J // [11.425,23.726000000000006,19.01833333333333,35.50857142857143,28.93166666666666,29.325000000000003,9.775,21.541249999999998,6.546666666666667,12.809999999999999,0,13.29,40.41166666666666,12.516,25.217142857142857,7.4174999999999995,7.968000000000001,53.916666666666664,27.663846153846155,12.25,11.325,16.88,31.11,16.79714285714286]
                    var buscar_datos_start = performance.now();
                    Factura.find({
                        nif: nif,
                        fecha: new Date("2021-03-19T00:00:00")
                    }, "cantidad xml", (err, query_dia_result) => {
                        var buscar_datos_fin = performance.now() - buscar_datos_start;
                        var tratar_facturas_start = performance.now();
                        var descomprimir_total=0;
                        var json = {"00:00:00" : {suma: 0,cantidad: 0,avg: 0}, "01:00:00" : {suma: 0,cantidad: 0,avg: 0}, "02:00:00" :{suma: 0,cantidad: 0,avg: 0},"03:00:00" : {suma: 0,cantidad: 0,avg: 0}, "04:00:00" : {suma: 0,cantidad: 0,avg: 0},"05:00:00" : {suma: 0,cantidad: 0,avg: 0}, "06:00:00" : {suma: 0,cantidad: 0,avg: 0},"07:00:00" : {suma: 0,cantidad: 0,avg: 0}, "08:00:00" : {suma: 0,cantidad: 0,avg: 0},"09:00:00" : {suma: 0,cantidad: 0,avg: 0},"10:00:00" : {suma: 0,cantidad: 0,avg: 0}, "11:00:00" : {suma: 0,cantidad: 0,avg: 0},"12:00:00" : {suma: 0,cantidad: 0,avg: 0}, "13:00:00" : {suma: 0,cantidad: 0,avg: 0}, "14:00:00" : {suma: 0,cantidad: 0,avg: 0}, "15:00:00" : {suma: 0,cantidad: 0,avg: 0}, "16:00:00" : {suma: 0,cantidad: 0,avg: 0}, "17:00:00" : {suma: 0,cantidad: 0,avg: 0}, "18:00:00" : {suma: 0,cantidad: 0,avg: 0}, "19:00:00" :{suma: 0,cantidad: 0,avg: 0}, "20:00:00" :{suma: 0,cantidad: 0,avg: 0}, "21:00:00" : {suma: 0,cantidad: 0,avg: 0}, "22:00:00" :{ suma: 0,cantidad: 0,avg: 0}, "23:00:00" :{suma: 0,cantidad: 0,avg: 0}};
                        query_dia_result.forEach((factura_com, index) => {
                            var descomprimir_start = performance.now();
                            let factura_descomp = zlib.gunzipSync(Buffer.from(factura_com.xml, "base64"), GZIP_PARAMS).toString();
                            descomprimir_total += (performance.now() - descomprimir_start);
                            let hora = DATA.getHoraExpedionFactura(factura_descomp);
                            let cantidad = factura_com.cantidad;
    
                            let hora_split = hora.split(":")[0];
                            
                            json[hora_split+':00:00'].suma += cantidad;
                            json[hora_split+':00:00'].cantidad ++;
                            json[hora_split+':00:00'].avg = json[hora_split+':00:00'].suma / json[hora_split+':00:00'].cantidad;  
                        });
                        var tratar_facturas_fin = performance.now() - tratar_facturas_start;
                        var dia_nif = [];
                        for(var i = 0; i < 24; i++){
                            let hora = i.toString().padStart(2,0);                     
                            dia_nif.push(json[hora+':00:00'].avg);
                        }
                        fs.appendFileSync('./files/estadisticas_diarias_stats_nif.txt', `BuscarDatosGlobal;DescomprimirFacturas;TratarFacturas;CalcularEstadisticaGlobal;BuscarDatosNIF\n${buscar_datos_fin};${descomprimir_total};${tratar_facturas_fin - descomprimir_total}\n`);
                        fs.appendFileSync('./estadisticas/2021-03-19_2021-03-19_global.txt', `dia_${nif} // [${dia_nif.toString()}]\n`);
                        resolve("OK");
                        /*var dia_nif = [];
                        var query_dia_descomp = query_dia_result.map(f => zlib.gunzipSync(Buffer.from(f.xml, "base64"), GZIP_PARAMS).toString());
                        for (var t = moment("00:00:00", "HH:mm:ss").toDate(); t <= moment("23:00:00", "HH:mm:ss").toDate(); t = moment(t).add(1, "hours").toDate()) {
                            let t_aux = t;
                            let nif_array = query_dia_descomp.filter(f => moment(DATA.getHoraExpedionFactura(f), "HH:mm:ss").toDate() >= t && moment(DATA.getHoraExpedionFactura(f), "HH:mm:ss").toDate() < moment(t_aux).add(1, "hours").toDate());
                            var nif_average = 0;
                            if (nif_array.length > 0) {
                                nif_average = nif_array.map(f => DATA.getImporteTotalFactura(f)).reduce((a, b) => a + b, 0) / nif_array.length;
                            }
                            dia_nif.push(nif_average);
                        }
                        fs.appendFileSync('./estadisticas/2021-03-19_2021-03-19_global.txt', `dia_${nif} // [${dia_nif.toString()}]\n`);
                        resolve("OK");
                        console.log(performance.now() - start);*/
                    });
                } else {
                    resolve("OK");
                }//end if
            } else {//No existe la estadistica global ni la del nif, asi que tengo que crearlas
                //console.log("No existe fichero");

                var nif_list = companies_nif_list.slice(0, 3000).map(c => c[0]);

                console.log("Busqueda start");
                var buscar_datos_start = performance.now();
                Factura.aggregate([
                    {
                        $match : {
                            nif: {
                                $in : nif_list
                            },
                            fecha: new Date("2021-03-19T00:00:00")
                        }
                    },
                    {
                        $project : {
                            _id : "$_id",
                            nif: "$nif",
                            fecha: "%fecha",
                            cantidad: "$cantidad",
                            xml : "$xml"
                        }
                    }
                ]).allowDiskUse().exec((err, query_dia_result) => {
                    if(err) throw err;
                    var buscar_datos_fin = performance.now() - buscar_datos_start;
                    console.log("End Busqueda datos");
                    console.log(query_dia_result.length);
                    const dia_labels = ["00:00:00", "01:00:00", "02:00:00", "03:00:00", "04:00:00", "05:00:00", "06:00:00", "07:00:00", "08:00:00", "09:00:00", "10:00:00", "11:00:00", "12:00:00", "13:00:00", "14:00:00", "15:00:00", "16:00:00", "17:00:00", "18:00:00", "19:00:00", "20:00:00", "21:00:00", "22:00:00", "23:00:00",];
                    var dia_nif = [];
                    var dia_sector = [];

                    var descomprimir_total = 0;
                    var json = {"00:00:00" : {suma: 0,cantidad: 0,avg: 0}, "01:00:00" : {suma: 0,cantidad: 0,avg: 0}, "02:00:00" :{suma: 0,cantidad: 0,avg: 0},"03:00:00" : {suma: 0,cantidad: 0,avg: 0}, "04:00:00" : {suma: 0,cantidad: 0,avg: 0},"05:00:00" : {suma: 0,cantidad: 0,avg: 0}, "06:00:00" : {suma: 0,cantidad: 0,avg: 0},"07:00:00" : {suma: 0,cantidad: 0,avg: 0}, "08:00:00" : {suma: 0,cantidad: 0,avg: 0},"09:00:00" : {suma: 0,cantidad: 0,avg: 0},"10:00:00" : {suma: 0,cantidad: 0,avg: 0}, "11:00:00" : {suma: 0,cantidad: 0,avg: 0},"12:00:00" : {suma: 0,cantidad: 0,avg: 0}, "13:00:00" : {suma: 0,cantidad: 0,avg: 0}, "14:00:00" : {suma: 0,cantidad: 0,avg: 0}, "15:00:00" : {suma: 0,cantidad: 0,avg: 0}, "16:00:00" : {suma: 0,cantidad: 0,avg: 0}, "17:00:00" : {suma: 0,cantidad: 0,avg: 0}, "18:00:00" : {suma: 0,cantidad: 0,avg: 0}, "19:00:00" :{suma: 0,cantidad: 0,avg: 0}, "20:00:00" :{suma: 0,cantidad: 0,avg: 0}, "21:00:00" : {suma: 0,cantidad: 0,avg: 0}, "22:00:00" :{ suma: 0,cantidad: 0,avg: 0}, "23:00:00" :{suma: 0,cantidad: 0,avg: 0}};
                    var json_nif = {"00:00:00" : {suma: 0,cantidad: 0,avg: 0}, "01:00:00" : {suma: 0,cantidad: 0,avg: 0}, "02:00:00" :{suma: 0,cantidad: 0,avg: 0},"03:00:00" : {suma: 0,cantidad: 0,avg: 0}, "04:00:00" : {suma: 0,cantidad: 0,avg: 0},"05:00:00" : {suma: 0,cantidad: 0,avg: 0}, "06:00:00" : {suma: 0,cantidad: 0,avg: 0},"07:00:00" : {suma: 0,cantidad: 0,avg: 0}, "08:00:00" : {suma: 0,cantidad: 0,avg: 0},"09:00:00" : {suma: 0,cantidad: 0,avg: 0},"10:00:00" : {suma: 0,cantidad: 0,avg: 0}, "11:00:00" : {suma: 0,cantidad: 0,avg: 0},"12:00:00" : {suma: 0,cantidad: 0,avg: 0}, "13:00:00" : {suma: 0,cantidad: 0,avg: 0}, "14:00:00" : {suma: 0,cantidad: 0,avg: 0}, "15:00:00" : {suma: 0,cantidad: 0,avg: 0}, "16:00:00" : {suma: 0,cantidad: 0,avg: 0}, "17:00:00" : {suma: 0,cantidad: 0,avg: 0}, "18:00:00" : {suma: 0,cantidad: 0,avg: 0}, "19:00:00" :{suma: 0,cantidad: 0,avg: 0}, "20:00:00" :{suma: 0,cantidad: 0,avg: 0}, "21:00:00" : {suma: 0,cantidad: 0,avg: 0}, "22:00:00" :{ suma: 0,cantidad: 0,avg: 0}, "23:00:00" :{suma: 0,cantidad: 0,avg: 0}};

                    var tratar_facturas_start = performance.now();
                    query_dia_result.forEach((factura_com, index) => {
                        var descomprimir_start = performance.now();
                        let factura_descomp = zlib.gunzipSync(Buffer.from(factura_com.xml, "base64"), GZIP_PARAMS).toString();
                        descomprimir_total += (performance.now() - descomprimir_start);
                        let hora = DATA.getHoraExpedionFactura(factura_descomp);
                        let cantidad = factura_com.cantidad;

                        let hora_split = hora.split(":")[0];
                        
                        json[hora_split+':00:00'].suma += cantidad;
                        json[hora_split+':00:00'].cantidad ++;
                        json[hora_split+':00:00'].avg = json[hora_split+':00:00'].suma / json[hora_split+':00:00'].cantidad;
                        if(factura_com.nif == nif){
                            json_nif[hora_split+':00:00'].suma += cantidad;
                            json_nif[hora_split+':00:00'].cantidad ++;
                            json_nif[hora_split+':00:00'].avg = json[hora_split+':00:00'].suma / json[hora_split+':00:00'].cantidad;
                        }

                    });
                    console.log("End tratar facturas");
                    var tratar_facturas_fin = performance.now() - tratar_facturas_start;

                    for(var i = 0; i < 24; i++){
                        let hora = i.toString().padStart(2,0);                     
                        dia_nif.push(json_nif[hora+':00:00'].avg);
                        dia_sector.push(json[hora+':00:00'].avg)
                    }

                    fs.appendFileSync('./files/estadisticas_diarias_stats.txt', `BuscarDatosGlobal;DescomprimirFacturas;TratarFacturas;CalcularEstadisticaGlobal;BuscarDatosNIF\n${buscar_datos_fin};${descomprimir_total};${tratar_facturas_fin - descomprimir_total}\n`);
                    fs.writeFileSync('./estadisticas/2021-03-19_2021-03-19_global.txt', `dia_labels // ["${dia_labels.join('","')}"]\ndia_sector // [${dia_sector.toString()}]\ndia_${nif} // [${dia_nif.toString()}]\n`);
                    resolve("OK");
                    /*
                    var tratar_facturas_start = performance.now();
                    query_dia_result.forEach((factura_com, index) => {
                        var descomprimir_start = performance.now();
                        let factura_descomp = zlib.gunzipSync(Buffer.from(factura_com.xml, "base64"), GZIP_PARAMS).toString();
                        descomprimir_total += (performance.now() - descomprimir_start);
                        let group = [DATA.getHoraExpedionFactura(factura_descomp), factura_com.cantidad];
                        hora_cantidad_global.push(group);

                        if (factura_com.nif == nif) {
                            //console.log("NIIIIIIIF");
                            let group_nif = [DATA.getHoraExpedionFactura(factura_descomp), factura_com.cantidad];
                            hora_cantidad_nif.push(group_nif);
                        }
                    });
                    console.log("End tratar facturas");
                    var tratar_facturas_fin = performance.now() - tratar_facturas_start;
                    var calcular_estadistica_start = performance.now();
                    //console.log(hora_cantidad_nif);
                    for (var t = moment("00:00:00", "HH:mm:ss").toDate(); t <= moment("23:00:00", "HH:mm:ss").toDate(); t = moment(t).add(1, "hours")) {
                        let t_plus_hour = t;

                        let filtro_global = hora_cantidad_global.filter(f => moment(f[0], "HH:mm:ss").toDate() >= moment(t).toDate() && moment(f[0], "HH:mm:ss").toDate() < moment(t_plus_hour).add(1, "hours").toDate());
                        console.log(filtro_global.length);
                        let sum = filtro_global.map(f => f[1]).reduce((a, b) => a + b, 0);

                        if (filtro_global.length == 0) {
                            dia_sector.push(0);
                        } else {
                            dia_sector.push(sum / filtro_global.length);
                        }

                        let filtro_nif = hora_cantidad_nif.filter(f => moment(f[0], "HH:mm:ss").toDate() >= moment(t).toDate() && moment(f[0], "HH:mm:ss").toDate() < moment(t_plus_hour).add(1, "hours").toDate());
                        let sum_nif = filtro_nif.map(f => f[1]).reduce((a, b) => a + b, 0);

                        if (filtro_nif.length == 0) {
                            dia_nif.push(0);
                        } else {
                            dia_nif.push(sum_nif / filtro_nif.length);
                        }
                        dia_labels.push(moment(t).format("HH:mm:ss"));
                    }
                    var calcular_estadistica_fin = performance.now() - calcular_estadistica_start;
                    console.log("End Calcular estadistica");
                    */
                });

            }//end if
        } catch (err) {

            //console.log(err);
            reject(err);
        }
    });
}

function createEstadisticaSemanal(nif) {
    //await mongoose.connect(mongoUrl + "/" + dbName).then(() => { console.log("Conexión a MongoDB realizada correctamente") });
    return new Promise((resolve, reject) => {
        console.log("Estadistica Semanal");
        //const DIRECTORY_PATH = "C:\\Users\\877205\\Desktop\\FacturasInsert\\insertData\\";
        //const index = fs.readFileSync(DIRECTORY_PATH + "index.txt").toString().split("\n");
        //const nif_list = index.map(n => n.split("/")[5].split(".")[0]);
        try {
            var start = performance.now();
            //console.log("Dentro del try");
            if (fs.existsSync("./estadisticas/2021-03-15_2021-03-21_global.txt")) {//Si existe la de un nif entonces existe la global
                //console.log("Existe fichero?");
                var global_semana_estadistica = fs.readFileSync("./estadisticas/2021-03-15_2021-03-21_global.txt").toString().split("\n");
                if (global_semana_estadistica.map(l => l.split(" // ")[0]).filter(n => n == `semana_${nif}`).length == 0) {//No existe la estadistica sobre ese nif
                    //console.log("No existe estadistica sobre el nif");
                    Factura.find({
                        nif: nif,
                        fecha: {
                            $gte: new Date("2021-03-15T00:00:00"),
                            $lte: new Date("2021-03-21T23:59:59")
                        }
                    }, "nif fecha cantidad", (err, query_semana_result) => {
                        var semana_nif = [];
                        //var query_dia_descomp = query_dia_result.map(f => zlib.gunzipSync(Buffer.from(f.xml, "base64"), GZIP_PARAMS).toString());
                        for (var t = moment("2021-03-15").toDate(); t <= moment("2021-03-21").toDate(); t = moment(t).add(1, "days").toDate()) {
                            let t_aux = t;
                            let nif_array = query_semana_result.filter(f => moment(f.fecha).toDate() >= t && moment(f.fecha).toDate() < moment(t_aux).add(1, "days").toDate());
                            var nif_average = 0;
                            if (nif_array.length > 0) {
                                nif_average = nif_array.map(f => f.cantidad).reduce((a, b) => a + b, 0) / nif_array.length;
                            }
                            semana_nif.push(nif_average);
                        }
                        fs.appendFileSync('./estadisticas/2021-03-15_2021-03-21_global.txt', `semana_${nif} // [${semana_nif.toString()}]\n`);
                        console.log(performance.now() - start);
                        resolve("OK");
                    });
                } else {
                    resolve("OK");
                }//end if
            } else {//No existe la estadistica global ni la del nif, asi que tengo que crearlas
                //console.log("No existe fichero");
                var tiempo_start = performance.now();
                var nif_list = companies_nif_list.map(c => c[0]).slice(0, 1000);
                Factura.find({
                    nif: {
                        $in: nif_list
                    },
                    fecha: {
                        $gte: new Date("2021-03-15T00:00:00"),
                        $lte: new Date("2021-03-21T23:59:59")
                    }
                }, "nif fecha cantidad", (err, query_semana_result) => {
                    var semana_labels = [];
                    var semana_nif = [];
                    var semana_sector = [];


                    for (var t = moment("2021-03-15").toDate(); t <= moment("2021-03-21").toDate(); t = moment(t).add(1, "days").toDate()) {
                        let t_aux = t;
                        let global_array = query_semana_result.filter(f => (moment(f.fecha).toDate() >= t) && (moment(f.fecha).toDate() < moment(t_aux).add(1, "days").toDate()));
                        var global_average = 0;
                        if (global_array.length > 0) {
                            global_average = global_array.map(f => f.cantidad).reduce((a, b) => a + b, 0) / global_array.length;
                        }
                        //console.log(global_array);

                        let nif_array = query_semana_result.filter(f => f.nif == nif).filter(f => moment(f.fecha).toDate() >= t && moment(f.fecha).toDate() < moment(t_aux).add(1, "days").toDate());
                        var nif_average = 0;
                        if (nif_array.length > 0) {
                            nif_average = nif_array.map(f => f.cantidad).reduce((a, b) => a + b, 0) / nif_array.length;
                        }
                        semana_nif.push(nif_average);
                        semana_sector.push(global_average);
                        semana_labels.push(moment(t).format("YYYY-MM-DD"));
                        console.log(semana_labels);
                    }
                    fs.writeFileSync('./estadisticas/2021-03-15_2021-03-21_global.txt', `semana_labels // ["${semana_labels.join('","')}"]\nsemana_sector // [${semana_sector.toString()}]\nsemana_${nif} // [${semana_nif.toString()}]\n`);
                    var tiempo_fin = performance.now() -tiempo_start;
                    console.log(tiempo_fin);
                    resolve("OK");
                });

            }//end if
        } catch (err) {

            //console.log(err);
            reject(err);
        }
    });
}

function createEstadisticaMensual(nif) {
    //await mongoose.connect(mongoUrl + "/" + dbName).then(() => { console.log("Conexión a MongoDB realizada correctamente") });
    return new Promise((resolve, reject) => {
        console.log("Estadistica Mensual");
        //const DIRECTORY_PATH = "C:\\Users\\877205\\Desktop\\FacturasInsert\\insertData\\";
        //const index = fs.readFileSync(DIRECTORY_PATH + "index.txt").toString().split("\n");
        //const nif_list = index.map(n => n.split("/")[5].split(".")[0]);
        try {
            var start = performance.now();
            //console.log("Dentro del try");
            if (fs.existsSync("./estadisticas/2021-03-01_2021-03-28_global.txt")) {//Si existe la de un nif entonces existe la global
                //console.log("Existe fichero?");
                var global_mes_estadistica = fs.readFileSync("./estadisticas/2021-03-01_2021-03-28_global.txt").toString().split("\n");
                if (global_mes_estadistica.map(l => l.split(" // ")[0]).filter(n => n == `mes_${nif}`).length == 0) {//No existe la estadistica sobre ese nif
                    //console.log("No existe estadistica sobre el nif");
                    Factura.find({
                        nif: nif,
                        fecha: {
                            $gte: new Date("2021-03-01T00:00:00"),
                            $lte: new Date("2021-03-28T23:59:59")
                        }
                    }, "nif fecha cantidad", (err, query_mes_result) => {
                        var mes_nif = [];
                        //var query_dia_descomp = query_dia_result.map(f => zlib.gunzipSync(Buffer.from(f.xml, "base64"), GZIP_PARAMS).toString());
                        for (var t = moment("2021-03-01").toDate(); t <= moment("2021-03-28").toDate(); t = moment(t).add(1, "days").toDate()) {
                            let t_aux = t;
                            let nif_array = query_mes_result.filter(f => moment(f.fecha).toDate() >= t && moment(f.fecha).toDate() < moment(t_aux).add(1, "days").toDate());
                            var nif_average = 0;
                            if (nif_array.length > 0) {
                                nif_average = nif_array.map(f => f.cantidad).reduce((a, b) => a + b, 0) / nif_array.length;
                            }
                            mes_nif.push(nif_average);
                        }
                        fs.appendFileSync('./estadisticas/2021-03-01_2021-03-28_global.txt', `mes_${nif} // [${mes_nif.toString()}]\n`);
                        console.log(performance.now() - start);
                        resolve("OK");
                    });
                } else {
                    resolve("OK");
                }//end if
            } else {//No existe la estadistica global ni la del nif, asi que tengo que crearlas
                //console.log("No existe fichero");
                var nif_list = companies_nif_list.map(c => c[0]).slice(0, 1000);
                Factura.find({
                    nif: {
                        $in: nif_list
                    },
                    fecha: {
                        $gte: new Date("2021-03-01T00:00:00"),
                        $lte: new Date("2021-03-28T23:59:59")
                    }
                }, "nif fecha cantidad", (err, query_mes_result) => {
                    var mes_labels = [];
                    var mes_nif = [];
                    var mes_sector = [];

                    for (var t = moment("2021-03-01").toDate(); t <= moment("2021-03-28").toDate(); t = moment(t).add(1, "days").toDate()) {
                        let t_aux = t;
                        let global_array = query_mes_result.filter(f => (moment(f.fecha).toDate() >= t) && (moment(f.fecha).toDate() < moment(t_aux).add(1, "days").toDate()));
                        var global_average = 0;
                        if (global_array.length > 0) {
                            global_average = global_array.map(f => f.cantidad).reduce((a, b) => a + b, 0) / global_array.length;
                        }
                        //console.log(global_array);

                        let nif_array = query_mes_result.filter(f => f.nif == nif).filter(f => moment(f.fecha).toDate() >= t && moment(f.fecha).toDate() < moment(t_aux).add(1, "days").toDate());
                        var nif_average = 0;
                        if (nif_array.length > 0) {
                            nif_average = nif_array.map(f => f.cantidad).reduce((a, b) => a + b, 0) / nif_array.length;
                        }
                        mes_nif.push(nif_average);
                        mes_sector.push(global_average);
                        mes_labels.push(moment(t).format("YYYY-MM-DD"));
                        console.log(mes_labels);//["' + labels.join('\","') + '"]
                    }
                    fs.writeFileSync('./estadisticas/2021-03-01_2021-03-28_global.txt', `mes_labels // ["${mes_labels.join('","')}"]\nmes_sector // [${mes_sector.toString()}]\nmes_${nif} // [${mes_nif.toString()}]\n`);
                    resolve("OK");
                });

            }//end if
        } catch (err) {

            //console.log(err);
            reject(err);
        }
    });
}



function createEstadisticaTrimestre(nif) {

    return new Promise(async (resolve, reject) => {
        console.log("Estadistica Trimestral");
        //const DIRECTORY_PATH = "C:\\Users\\877205\\Desktop\\FacturasInsert\\insertData\\";
        //const index = fs.readFileSync(DIRECTORY_PATH + "index.txt").toString().split("\n");
        //const nif_list = index.map(n => n.split("/")[5].split(".")[0]);
        try {
            if (fs.existsSync("./estadisticas/2021-01-01_2021-03-28_global.txt")) {//Si existe la de un nif entonces existe la global
                //console.log("Existe fichero?");
                var global_mes_estadistica = fs.readFileSync("./estadisticas/2021-01-01_2021-03-28_global.txt").toString().split("\n");
                if (global_mes_estadistica.map(l => l.split(" // ")[0]).filter(n => n == `triMes_${nif}`).length == 0) {//No existe la estadistica sobre ese nif
                    //console.log("No existe estadistica sobre el nif");
                    var nif_sum = [];
                    for (var t = moment("2021-01-01").toDate(); t < moment("2021-04-01").toDate(); t = moment(t).add(1, "months").toDate()) {
                        let t_plus_month = moment(t).add(1, "months").subtract(1, "days").toDate();
                        //let t_plus_month = moment(t).add(1, "months").toDate();


                        let resul = await FacturaAgrupada.findOne({
                            nif: nif,
                            fechaInicio: { $eq: t },
                            fechaFin: { $eq: t_plus_month }
                        }, "agrupacion").exec();
                        try {
                            let facturas_array = zlib.gunzipSync(Buffer.from(resul.agrupacion, "base64"), GZIP_PARAMS).toString().split(/(?=\<\?xml version="1\.0" encoding="utf-8"\?\>)/);

                            facturas_array.forEach((factura, index) => {
                                let fecha = moment(DATA.getFechaExp(factura), "DD-MM-YYYY").format("YYYY-MM-DD");
                                let importe = DATA.getImporteTotalFactura(factura);
                                nif_sum.push([fecha, importe]);

                            });
                        } catch (err) {
                            console.log("Error en nif --> " + nif);
                        }




                    }//end for



                    var mes_nif = [];

                    //Recorro todos los dias.
                    for (var t = moment("2021-01-01").toDate(); t <= moment("2021-03-28").toDate(); t = moment(t).add(1, "days").toDate()) {
                        let nif_array = nif_sum.filter(e => e[0] == moment(t).format("YYYY-MM-DD")).map(e => e[1]);
                        if (nif_array.length > 0) {
                            mes_nif.push(nif_array.reduce((a, b) => a + b, 0) / nif_array.length);
                        } else {
                            mes_nif.push(0);
                        }
                        //mes_nif.push(nif_sum.filter(e => e[0] == moment(t).format("YYYY-MM-DD")).map(e => e[1]).reduce((a,b) => a + b ,0));
                    }

                    fs.appendFileSync('./estadisticas/2021-01-01_2021-03-28_global.txt', `triMes_${nif} // [${mes_nif.toString()}]\n`);
                    resolve("OK");
                } else {
                    resolve("OK");
                }//end if
            } else {//No existe la estadistica global ni la del nif, asi que tengo que crearlas
                //console.log("No existe fichero");
                var nif_list = companies_nif_list.slice(0, 1000).map(c => c[0]);
                //var nif_list = companies_nif_list.slice(0, 2).map(c => c[0]);
                var global_sum = [];
                var nif_sum = [];
                for (var t = moment("2021-01-01").toDate(); t < moment("2021-04-01").toDate(); t = moment(t).add(1, "months").toDate()) {
                    //nif_list.forEach((nif_l, index) => {
                    for (var j = 0; j < nif_list.length; j++) {
                        let nif_l = nif_list[j];
                        let t_plus_month = moment(t).add(1, "months").subtract(1, "days").toDate();

                        let resul = await FacturaAgrupada.findOne({
                            nif: nif_l,
                            fechaInicio: { $lte: t },
                            fechaFin: { $gte: t_plus_month }
                        }, "agrupacion").exec();

                        try {
                            let facturas_array = zlib.gunzipSync(Buffer.from(resul.agrupacion, "base64"), GZIP_PARAMS).toString().split(/(?=\<\?xml version="1\.0" encoding="utf-8"\?\>)/);
                            facturas_array.forEach((factura, index) => {
                                //console.log(factura);
                                let fecha = moment(DATA.getFechaExp(factura), "DD-MM-YYYY").format("YYYY-MM-DD");
                                let importe = DATA.getImporteTotalFactura(factura);

                                global_sum.push([fecha, importe]);
                                if (nif_l == nif) {
                                    nif_sum.push([fecha, importe]);
                                }
                            });
                        } catch (err) {
                            console.log("Error al descomprimir el grupo --> " + nif_l);
                        }

                    }
                    console.log(moment(t).format("YYYY-MM-DD"));
                }//end for


                var mes_labels = [];
                var mes_nif = [];
                var mes_sector = [];
                //Recorro todos los dias.
                for (var t = moment("2021-01-01").toDate(); t <= moment("2021-03-28").toDate(); t = moment(t).add(1, "days").toDate()) {

                    let mes_array = global_sum.filter(e => e[0] == moment(t).format("YYYY-MM-DD")).map(e => e[1]);
                    if (mes_array.length > 0) {
                        mes_sector.push(mes_array.reduce((a, b) => a + b, 0) / mes_array.length);
                    } else {
                        mes_sector.push(0);
                    }

                    let nif_array = nif_sum.filter(e => e[0] == moment(t).format("YYYY-MM-DD")).map(e => e[1]);
                    if (nif_array.length > 0) {
                        mes_nif.push(nif_array.reduce((a, b) => a + b, 0) / nif_array.length);
                    } else {
                        mes_nif.push(0);
                    }

                    mes_labels.push(moment(t).format("YYYY-MM-DD"));
                }

                fs.appendFileSync('./estadisticas/2021-01-01_2021-03-28_global.txt', `triMes_labels // ["${mes_labels.join('","')}"]\ntriMes_sector // [${mes_sector.toString()}]\ntriMes_${nif} // [${mes_nif.toString()}]\n`);
                resolve("OK");

            }//end if
        } catch (err) {

            //console.log(err);
            reject(err);
        }
    });
}

async function estadisticasHosteleria(nif) {

    try {
        const res = await createEstadisticaDiaria(nif);
        console.log("Dia --> " + res);
        try {
            const res_1 = await createEstadisticaSemanal(nif);
            console.log("Semana --> " + res_1);
            try {
                const res_2 = await createEstadisticaMensual(nif);
                console.log("Mes --> " + res_2);
                try {
                    const res_3 = await createEstadisticaTrimestre(nif);
                    var file = fs.readFileSync('./estadisticas/2021-03-19_2021-03-19_global.txt').toString().split('\n');
                    var dia_labels = JSON.parse(file[0].split(" // ")[1]);
                    var dia_sector = JSON.parse(file[1].split(" // ")[1]);
                    var dia_nif = JSON.parse(file.filter(l => l.split(" // ")[0] == `dia_${nif}`)[0].split(" // ")[1]);

                    var semana_file = fs.readFileSync('./estadisticas/2021-03-15_2021-03-21_global.txt').toString().split('\n');
                    var semana_labels = JSON.parse(semana_file[0].split(" // ")[1]);
                    var semana_sector = JSON.parse(semana_file[1].split(" // ")[1]);
                    var semana_nif = JSON.parse(semana_file.filter(l_1 => l_1.split(" // ")[0] == `semana_${nif}`)[0].split(" // ")[1]);

                    var mes_file = fs.readFileSync('./estadisticas/2021-03-01_2021-03-28_global.txt').toString().split('\n');
                    var mes_labels = JSON.parse(mes_file[0].split(" // ")[1]);
                    var mes_sector = JSON.parse(mes_file[1].split(" // ")[1]);
                    var mes_nif = JSON.parse(mes_file.filter(l_1 => l_1.split(" // ")[0] == `mes_${nif}`)[0].split(" // ")[1]);

                    var triMes_file = fs.readFileSync('./estadisticas/2021-01-01_2021-03-28_global.txt').toString().split('\n');
                    var triMes_labels = JSON.parse(triMes_file[0].split(" // ")[1]);
                    var triMes_sector = JSON.parse(triMes_file[1].split(" // ")[1]);
                    var triMes_nif = JSON.parse(triMes_file.filter(l_1 => l_1.split(" // ")[0] == `triMes_${nif}`)[0].split(" // ")[1]);



                    return {
                        dia_nif: dia_nif,
                        dia_sector: dia_sector,
                        dia_labels: dia_labels,
                        semana_nif: semana_nif,
                        semana_sector: semana_sector,
                        semana_labels: semana_labels,
                        mes_nif: mes_nif,
                        mes_sector: mes_sector,
                        mes_labels: mes_labels,
                        triMes_labels: triMes_labels,
                        triMes_nif: triMes_nif,
                        triMes_sector: triMes_sector
                    };
                } catch (err_3) {
                    throw err_3;
                }
            } catch (err_2) {
                throw err_2
            }
        } catch (err) {
            throw err;
        }
    } catch (err_1) {
        throw err_1;
    }
}

async function pruebasEstadisticasHosteleria() {

    /** QUERY SUMA TOTAL FACTURAS CON FILTRO EN RAW */
    const nif_list = ["15964763A", "24624714V", "53766353N", "88326204V", "18650180D"];


    for(var i = 0; i < nif_list.length; i++){
        let nif = nif_list[i];
        for(var j = 0; j < 5; j++){
            var busqueda_facturas_filtro_start = performance.now();
            await Factura.aggregate([
                {
                    "$match": {
                        "nif": nif,
                        "cantidad": {
                            "$gte": 10
                        }
                    }
                },
                {
                    "$group": {
                        "_id": null,
                        "Total": {
                            "$sum": "$cantidad"
                        }
                    }
                }
            ]).exec();
            var busqueda_facturas_filtro_fin = performance.now();

            var obtener_facturas_start = performance.now();
            let query_2_result = await executeQuery({ nif: nif });
            var obtener_facturas_fin = performance.now();
        
            var array_facturas_descomp = [];
            //var tiempo_descomprimir = 0;
            var descomprimir_start = performance.now();
            for (var k = 0; k < query_2_result.length; k++) {
        
                let factura_descomp = await unCompressData(query_2_result[k].xml).catch((err) => { console.log("Error al descomprimir en query_2") });
                //tiempo_descomprimir += (performance.now()-descomprimir_start);
                //let json = parser.xml2json(factura_descomp, {compact: true, ignoreAttributes: true, ignoreDeclaration: true, spaces: '\t'});
                array_facturas_descomp.push(factura_descomp);
            }
            var descomprimir_fin = performance.now();
        
            //console.log(array_facturas_descomp[0]);
            var sumar_start = performance.now();
            let suma = array_facturas_descomp.map(f => DATA.getImporteTotalFactura(f)).filter(i => i >= 10).reduce((a, b) => a + b, 0);
            var sumar_fin = performance.now();
            //console.log(suma);
            fs.writeFileSync("./files/estadisticas_hosteleria.csv", nif + ";" + (busqueda_facturas_filtro_fin - busqueda_facturas_filtro_start) + ";" + (obtener_facturas_fin - obtener_facturas_start) + ";" + (descomprimir_fin - descomprimir_start) + ";" + (sumar_fin - sumar_start) + "\n", { flag: "a" });
        } 
    }
        


    


    console.log("OK");

}


module.exports = controller;