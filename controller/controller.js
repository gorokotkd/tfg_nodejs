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
const MesAgrupadas = require('../model/mes_agrupadas');
const SemanaAgrupadas = require('../model/semana_agrupada');
const TriMesAgrupadas = require('../model/trimes_agrupada');
const DayAgrupadas = require('../model/dia_agrupadas');
const { json } = require('express');
const { resolve } = require('path');
const dia_agrupadas = require('../model/dia_agrupadas');



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
            if (error) {
                reject(error);
            } else {
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
        await createFacturas.createData();
        res.send("OK");
    },
    insertFactura: function (req, res) {
        return res.status(200).render(
            'insert',
            {
                title: 'Inserción de Facturas',
                page: 'Inserción de Facturas'
            }
        );
    },
    insertManyView: async function (req, res) {
        res.status(200).render('insertMany', {
            title: 'Inserción múltiple de facturas',
            page: 'Inserción Múltiple de Facturas'
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
            page: 'Estadísticas Sector al Por Menor'
        });
    },
    showStatisticsMayor: function (req, res) {
        res.status(200).render('estadisticasPorMayor', {
            title: 'Estadísticas por Sector',
            page: 'Estadísticas Sector al Por Mayor'
        });
    },
    showStatistics: async function (req, res) {
        let sector = req.query.sector;
        let nif = req.query.nif;
        let resul;
        const regex = /^[0-9]{8}[A-Z]$/;
        if (!String(nif).match(regex)) {//El nif no tiene el formato correcto
            res.status(400).send("El nif no tiene el formato correcto (8 números seguidos de una letra mayúscula)");
        } else {
            if (await existeNif(nif)) {
                switch (sector) {
                    case "hosteleria":
                        resul = await estadisticasHosteleria(nif);
                        //console.log(resul);
                        if (resul == null) {
                            res.status(500).send("Error");
                        }
                        res.status(200).send(resul);
                        break;
                    case "maquinaria":
                        resul = await estadisticasMaquinaria(nif);
                        if (resul == null) {
                            res.status(500).send("Error");
                        }
                        res.status(200).send(resul);
                        break;
                    default:
                        res.status(400).send("Incorrent Query");
                        break;
                }
            } else {
                res.status(400).send("El nif introducido no se encuentra registrado.");
            }

        }

    },
    insertMany: async function (req, res) {

        var num_agrupadas = req.query.num;
        insertManyAgrupadas(num_agrupadas).then(resul => {
            res.status(200).send(resul);
        }).catch(err => {
            res.status(500).send(err);
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

        res.status(200).render('getfactura', {
            title: 'Búsqueda de Facturas',
            page: 'Búsqueda de Facturas'
        });
    },
    gr: async function (req, res) {
        const regex = /^TBAI-[0-9]{8}[A-Z]-[0-9]{6}-.{13}-[0-9]{3}$/;
        //document.getElementById('loading_gif').style.display = "block";
        var tbai_id = String(req.query.id);
        if (!tbai_id.match(regex)) {
            res.status(200).render('error', {
                title: 'Búsqueda de Facturas',
                page: 'Búsqueda de Facturas',
                error: {
                    status: 400,
                    reason: "El identificador de la factura está mal formado."
                }

            });
        } else {

            const client = new cassandra.Client({
                contactPoints: ['127.0.0.1'],
                keyspace: 'ticketbai',
                localDataCenter: 'datacenter1',
                queryOptions: { consistency: 1 },
                socketOptions: { readTimeout: 0 }
            });

            if (tbai_id.split("-")[1] == "15964763A") {
                //Busco dentro de la agrupacion. Es simplemente para que busque las facturas de ese nif dentro de la coleccion de 
                //facturas agrupadas en lugar de la de facturas individuales.
                let nif = tbai_id.split("-")[1];
                let fecha = tbai_id.split("-")[2];
                var busqueda_datos_start = performance.now();
                let docs = await FacturaAgrupada.find({
                    nif: nif,
                    fechaInicio: {
                        $lte: moment(fecha, "DDMMYY").toDate()
                    },
                    fechaFin: {
                        $gte: moment(fecha, "DDMMYY").toDate()
                    }
                });
                var busqueda_datos_fin = performance.now();
                for (var i = 0; i < docs.length; i++) {
                    if (docs[i].idents.includes(tbai_id)) {
                        var pos = Array.from(docs[i].idents).indexOf(tbai_id);
                        var agrupacion = docs[i].agrupacion;
                        var descompresion_start = performance.now();

                        let resul = zlib.gunzipSync(Buffer.from(agrupacion, "base64"), GZIP_PARAMS).toString();
                        var descompresion_fin = performance.now();

                        var busqueda_factura_start = performance.now();
                        var facturas_array = resul.split(/(?=\<\?xml version="1\.0" encoding="utf-8"\?\>)/);
                        let data = facturas_array[pos];
                        var busqueda_factura_fin = performance.now();


                        var result_mongo = {
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
                        };

                    }
                }//END FOR
                //END BUSQUEDA MONGO

                try {
                    var result_cassandra = await findAgrupadasCassandra(tbai_id, client);
                } catch (err) {
                    var result_cassandra = {
                        stats: {}
                    };
                }



            } else {
                var result_mongo = await findByTBAI(tbai_id);

                try {
                    var result_cassandra = await findByIdCassandra(tbai_id, client);
                } catch (err) {

                    var result_cassandra = {
                        stats: {}
                    };
                }
            }




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
                        backgroundColor: ["rgba(255, 99, 132, 1)", "rgba(54, 162, 235, 1)"],
                        borderColor: ["rgb(255, 99, 132)","rgb(54, 162, 235)"],
                        borderWidth: 1
                    }]
                };

                const config_decom = {
                    type: "bar",
                    data: data_decom,
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
                var chart = new Chart(ctx_decom, config_decom);</script>`;

                script_busqueda_fact = `<script>
                var ctx_busqueda_fact = document.getElementById("recuperacion-agrupacion-chart");
                const labels_busqueda_fact = ["MongoDB", "Cassandra"];
                const data_busqueda_fact = {
                    labels: labels_busqueda_fact,
                    datasets:[{
                        label: "Tiempo de Búsqueda en la Agrupación (milisegundos)",
                        data: [ ${(result_mongo.stats.busqueda_factura)}, ${(result_cassandra.stats.busqueda_factura)}],
                        backgroundColor: ["rgba(255, 99, 132, 1)", "rgba(54, 162, 235, 1)"],
                        borderColor: ["rgb(255, 99, 132)","rgb(54, 162, 235)"],
                        borderWidth: 1
                    }]
                };

                const config_busqueda_fact = {
                type: "bar",
                data: data_busqueda_fact,
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
                var chart = new Chart(ctx_busqueda_fact, config_busqueda_fact);</script>`;
            }

            let num_factura = "00000120210101";
            if (result_mongo.data.num_factura != null) {
                num_factura = result_mongo.data.num_factura;
            }

            res.status(200).render('gr', {
                title: 'Búsqueda de Facturas',
                page: 'Búsqueda de Facturas',
                error: '<script>document.getElementById("data").style.display = "block";</script><script>document.getElementById("tbai_error").style.display = "none";</script>',
                tbai_id: result_mongo.data.tbai_id,
                nif_emisor: result_mongo.data.nif_emisor,
                serie_factura: result_mongo.data.serie_factura,
                num_factura: num_factura,
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
                            page: 'Inserción de Facturas',
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
                    page: 'Inserción de Facturas'
                });


            } else {//Error el formato no es correcto
                res.status(200).send({
                    tbai_id: -2,
                    title: 'Inserción de Facturas',
                    page: 'Inserción de Facturas'
                });
            }

        } else {//error al enviar los archivos
            res.status(200).send(
                {
                    title: 'Inserción de Facturas',
                    page: 'Inserción de Facturas',
                    tbai_id: -3
                }
            );
        }
    },
    insertFacturasEstadisticas: async function (req, res) {

        //const DIRECTORY_PATH = "/Users/gorkaalvarez/Desktop/tbaiData/";
        const DIRECTORY_PATH = "C:\\Users\\877205\\Desktop\\tbaiData\\";

        const index = fs.readFileSync(DIRECTORY_PATH + "index.txt").toString().split("\n");
        const client = new cassandra.Client({
            contactPoints: ['127.0.0.1'],
            keyspace: 'ticketbai',
            localDataCenter: 'datacenter1'
        });

        for (var i = 0; i < index.length; i++) {
            //let nif = companies_nif_list[i][0];
            //let file = index[i].split("/")[5];
            let file = index[i];
            //console.log(DIRECTORY_PATH+file);
            try {
                var facturas = JSON.parse(fs.readFileSync(DIRECTORY_PATH + file).toString());
                //await insert_mongo(facturas).then(() => {console.log("Guardada --> "+file)}).catch(() => {console.log("Error al guardar --> "+file)});
            } catch (err) {
                console.log("Error al leer la factura --> " + file);
            }

            var array = [];
            //console.log(facturas.length);
            for (var j = 0; j < facturas.length; j++) {
                let factura_j = facturas[j];
                let data = {};
                //data._id = factura_j.id_tbai;
                data._id = factura_j._id;
                data.nif = factura_j.nif;
                data.fecha = moment(factura_j.fecha).toDate();
                data.cantidad = factura_j.cantidad;
                data.serie = factura_j.serie;
                data.status = factura_j.status;
                data.xml = factura_j.xml;
                array.push(data);
                /*
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
               */
            }
            //console.log("Guardada --> " + file);
            await insert_mongo(array).then(() => { console.log("Guardada --> " + file) }).catch(() => { console.log("Error al guardar --> " + file) });




        }

        res.status(200).send("OK");

    },
    agruparFacturas: async function (req, res) {
        try {
            console.log("INICIO AGRUPAR DIA");
            await agruparDia();
            /* console.log("INICIO AGRUPAR SEMANA");
             await agruparSemana();
             console.log("INICIO AGRUPAR MES");
             await agruparMes();
             console.log("INICIO AGRUPAR TRIMES");
             await agruparTrimes();*/
            res.status(200).send("OK");
        } catch (err) {

        }
    }

};


async function existeNif(nif) {
    let res = await Factura.findOne({
        nif: nif
    });

    if (res == null) {
        return false;
    } else {
        return true;
    }
}

function insertManyAgrupadas(num_agrupadas) {
    return new Promise((resolve, reject) => {
        const client = new cassandra.Client({
            contactPoints: ['127.0.0.1'],
            keyspace: 'ticketbai',
            localDataCenter: 'datacenter1'
        });
        var tbai_list = [];

        const nif = "15964763A";
        Factura.find({
            nif: nif
        }, "_id xml").limit(Number(num_agrupadas)).exec((err, facturas_bd) => {
            var agrupacion = "";
            var fecha_inicio_agrupacion;
            var fecha_fin_agrupacion;
            for (var i = 0; i < facturas_bd.length; i++) {
                let factura = zlib.gunzipSync(Buffer.from(facturas_bd[i].xml, "base64"), GZIP_PARAMS).toString();
                if (i == 0) {
                    fecha_inicio_agrupacion = DATA.getFechaExp(factura);
                } else if (i == facturas_bd.length - 1) {
                    fecha_fin_agrupacion = DATA.getFechaExp(factura);
                }
                agrupacion += factura;
                tbai_list.push(facturas_bd[i]._id);
            }


            var compresion_start = performance.now();
            var agrupacion_compress = zlib.gzipSync(agrupacion, GZIP_PARAMS).toString("base64"); //await compressData(agrupacion);
            var compresion_fin = performance.now();


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
                    for (var k = Math.round(((j * num_agrupadas) / numParticiones)); k < Math.round((j + 1) * num_agrupadas) / numParticiones; k++) {
                        let factura = zlib.gunzipSync(Buffer.from(facturas_bd[k].xml, "base64"), GZIP_PARAMS).toString();
                        agrupacion_mongo += factura;
                        tbai_part_list.push(DATA.getIdentTBAI(factura));
                    }
                    var compress_mongo_start = performance.now();
                    let agrupacion_mongo_compress = zlib.gzipSync(agrupacion_mongo, GZIP_PARAMS).toString("base64"); //await compressData(agrupacion_mongo);
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
            var insert_mongo_start = performance.now();
            //await insert_agrupadas_mongo(insert_array);
            const group = new AgrupacionFactura();
            group.collection.insertMany(insert_array, { ordered: false }, (err, docs) => {
                var insert_mongo_fin = performance.now();
                if (err) { console.log(err); }



                const insert_query = "insert into facturas_agrupadas (nif, fecha_inicio, agrupacion, fecha_fin, tbai_id_list) values (?,?,?,?,?)";
                const params = [
                    nif,
                    moment(fecha_inicio_agrupacion, "DD-MM-YYYY").format("YYYY-MM-DD"),
                    agrupacion_compress,
                    moment(fecha_fin_agrupacion, "DD-MM-YYYY").format("YYYY-MM-DD"),
                    tbai_list
                ];
                console.log("INSERT Cassandra");
                var insertar_cassandra_start = performance.now();
                try {
                    client.execute(insert_query, params, { prepare: true }).then(resul => {
                        var insert_cassandra_fin = performance.now();
                        resolve({
                            tbai_id: tbai_list[tbai_list.length - 1],
                            stats: {
                                insert_cassandra: insert_cassandra_fin - insertar_cassandra_start,
                                insert_mongo: insert_mongo_fin - insert_mongo_start,
                                comprimir_total_cassandra: compresion_fin - compresion_start,
                                comprimir_mongo: comprimir_mongo
                            }
                        });

                    }).catch(err => {
                        reject(err);
                    });
                } catch (err) {
                    reject(err);
                }
            });

        });

    });
}

async function estadisticasMaquinaria(nif) {
    try {
        const res = await calcularEstadisticasMaquinaria(nif);
        console.log(res);
        const file = fs.readFileSync('./estadisticas/2021-03-01_2021-03-28_paises.txt').toString().split("\n");
        var labels = JSON.parse(file[0].split(" // ")[1]);
        var ingresos_sector = JSON.parse(file[1].split(" // ")[1]);
        var exportaciones_sector = JSON.parse(file[2].split(" // ")[1]);
        var ingresos_nif = JSON.parse(file.filter(l => l.split(" // ")[0] == `maquinaria_ingresos_${nif}`)[0].split(" // ")[1]);
        var exportaciones_nif = JSON.parse(file.filter(l => l.split(" // ")[0] == `maquinaria_exportaciones_${nif}`)[0].split(" // ")[1]);
        return {
            labels: labels,
            exportaciones_sector: exportaciones_sector,
            exportaciones_nif: exportaciones_nif,
            ingresos_sector: ingresos_sector,
            ingresos_nif: ingresos_nif
        }
    } catch (err) {
        return null;
    }
}


function calcularEstadisticasMaquinaria(nif) {
    return new Promise((resolve, reject) => {
        if (fs.existsSync('./estadisticas/2021-03-01_2021-03-28_paises.txt')) {//La estadistica del sector esta hecha
            const file = fs.readFileSync('./estadisticas/2021-03-01_2021-03-28_paises.txt').toString().split("\n");
            var ingresos_nif = file.filter(l => l.split(" // ")[0] == `maquinaria_ingresos_${nif}`);
            if (ingresos_nif.length == 0) {//Tengo que calcular la estadistica para el NIF
                Factura.find({
                    nif: nif,
                    fecha: {
                        $gte: new Date("2021-03-01T00:00:00"),
                        $lte: new Date("2021-03-28T00:00:00")
                    }
                }, "xml", (err, facturas) => {
                    if (err) reject(err);

                    var json_nif = {};
                    var labels = [];

                    facturas.forEach((factura_com, index) => {
                        let factura = zlib.gunzipSync(Buffer.from(factura_com.xml, "base64"), GZIP_PARAMS).toString();
                        let pais = DATA.getCodigoPais(factura);
                        let ingresos = DATA.getImporteTotalFactura(factura);

                        var pais_name = "";
                        if (pais == "" || pais == null) {
                            pais_name = "ES";
                            if (!json_nif.hasOwnProperty('ES')) {
                                json_nif['ES'] = {
                                    nifs: [],
                                    exportaciones: 0,
                                    ingresos: 0
                                };
                                labels.push('ES');
                            }
                        } else {
                            if (!json_nif.hasOwnProperty(pais)) {
                                json_nif[pais] = {
                                    nifs: [],
                                    exportaciones: 0,
                                    ingresos: 0
                                };
                                labels.push(pais);
                            }
                            pais_name = pais
                        }

                        json_nif[pais_name].exportaciones++;
                        json_nif[pais_name].ingresos += ingresos;


                    });//End ForEach

                    var nif_exportaciones_data = [];
                    var nif_ingresos_data = [];

                    labels.forEach((pais, index) => {
                        nif_exportaciones_data.push(json_nif[pais].exportaciones);
                        nif_ingresos_data.push(json_nif[pais].ingresos / json_nif[pais].exportaciones);
                    });
                    fs.appendFileSync('./estadisticas/2021-03-01_2021-03-28_paises.txt', `maquinaria_ingresos_${nif} // [${nif_ingresos_data.toString()}]\nmaquinaria_exportaciones_${nif} // [${nif_exportaciones_data.toString()}]\n`);
                    resolve("OK");
                });


            } else {//La estadistica ya esta calculada
                resolve("OK");
            }

        } else {//No esta la del sector ni la del nif que me piden
            const nif_array = companies_nif_list.slice(4000, 4070).map(n => n[0]);
            Factura.find({
                nif: {
                    $in: nif_array
                },
                fecha: {
                    $gte: new Date("2021-03-01T00:00:00"),
                    $lte: new Date("2021-03-28T00:00:00")
                }
            }, "nif xml", (err, facturas) => {
                if (err) reject(err);
                var json_nif = {};
                var json_sector = {};
                var labels = [];
                facturas.forEach((factura_comp, index) => {
                    let factura = zlib.gunzipSync(Buffer.from(factura_comp.xml, "base64"), GZIP_PARAMS).toString();
                    let pais = DATA.getCodigoPais(factura);
                    let ingresos = DATA.getImporteTotalFactura(factura);

                    var pais_name = "";
                    if (pais == "" || pais == null) {
                        pais_name = "ES";
                        if (!json_sector.hasOwnProperty('ES')) {
                            json_sector['ES'] = {
                                nifs: [],
                                exportaciones: 0,
                                ingresos: 0
                            };
                            labels.push('ES');
                        }
                    } else {
                        if (!json_sector.hasOwnProperty(pais)) {
                            json_sector[pais] = {
                                nifs: [],
                                exportaciones: 0,
                                ingresos: 0
                            };
                            labels.push(pais);
                        }
                        pais_name = pais
                    }

                    if (!json_sector[pais_name].nifs.includes(factura_comp.nif)) {
                        json_sector[pais_name].nifs.push(factura_comp.nif);
                    }

                    json_sector[pais_name].exportaciones++;
                    json_sector[pais_name].ingresos += ingresos;

                    if (factura_comp.nif == nif) {
                        if (!json_nif.hasOwnProperty(pais_name)) {
                            json_nif[pais_name] = {
                                exportaciones: 0,
                                ingresos: 0
                            };
                        }

                        json_nif[pais_name].exportaciones++;
                        json_nif[pais_name].ingresos += ingresos;
                    }


                });//End ForEach

                var sector_exportaciones_data = [];
                var nif_exportaciones_data = [];
                var sector_ingresos_data = [];
                var nif_ingresos_data = [];

                labels.forEach((pais, index) => {

                    nif_exportaciones_data.push(json_nif[pais].exportaciones);
                    nif_ingresos_data.push(json_nif[pais].ingresos / json_nif[pais].exportaciones);

                    sector_exportaciones_data.push(json_sector[pais].exportaciones / json_sector[pais].nifs.length);
                    sector_ingresos_data.push(json_sector[pais].ingresos / json_sector[pais].exportaciones);

                });


                fs.appendFileSync('./estadisticas/2021-03-01_2021-03-28_paises.txt', `maquinaria_labels // ["${labels.join('","')}"]\nmaquinaria_ingresos_sector // [${sector_ingresos_data.toString()}]\nmaquinaria_exportaciones_sector // [${sector_exportaciones_data.toString()}]\nmaquinaria_ingresos_${nif} // [${nif_ingresos_data.toString()}]\nmaquinaria_exportaciones_${nif} // [${nif_exportaciones_data.toString()}]\n`);

                resolve("OK");
            });
        }
    });
}

function agruparMes() {
    return new Promise(async (resolve, reject) => {
        var nif_array = companies_nif_list.slice(0, 3000).map(n => n[0]);
        //var nif_array = ["15964763A"];
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


                    await new MesAgrupadas().collection.insertMany([data_to_insert], { ordered: false }, (err, docs) => {
                        if (err) { console.log("Error en nif " + nif_array[i] + "ERROR --> " + err); }
                    });

                } else {
                    console.log("Error al transformar las facturas del nif --> " + nif_array[i]);
                }
            }
            //console.log("Insertado NIF --> " + nif_array[i]);
        }
        resolve("OK");
    });
}

function agruparSemana() {
    return new Promise(async resolve => {
        var nif_array = companies_nif_list.slice(0, 3000).map(n => n[0]);
        for (var i = 0; i < nif_array.length; i++) {

            for (var t = moment("2021-01-04").toDate(); t < moment("2021-03-29").toDate(); t = moment(t).add(1, "weeks").toDate()) {
                let t_aux = moment(t);
                let facturas = await Factura.find({
                    nif: nif_array[i],
                    fecha: {
                        $gte: t,
                        $lte: t_aux.add(1, "weeks").toDate()
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
                    //await insert_agrupadas_mongo([data_to_insert]);

                    await new SemanaAgrupadas().collection.insertMany([data_to_insert], { ordered: false }, (err, docs) => {
                        if (err) { console.log("Error en nif " + nif_array[i] + "ERROR --> " + err); }
                    });

                } else {
                    console.log("Error al transformar las facturas del nif --> " + nif_array[i]);
                }
            }
            //console.log("Insertado NIF --> " + nif_array[i]);
        }

        resolve("OK");
    });

}

function agruparDia() {
    return new Promise(async resolve => {
        var nif_array = companies_nif_list.slice(0, 3000).map(n => n[0]);
        for (var i = 0; i < nif_array.length; i++) {

            for (var t = moment("2021-01-04").toDate(); t < moment("2021-03-29").toDate(); t = moment(t).add(1, "days").toDate()) {
                let t_aux = moment(t);
                let facturas = await Factura.find({
                    nif: nif_array[i],
                    fecha: t
                }, "xml").exec();
                //console.log(facturas);

                if (facturas != null && facturas.length > 0) {
                    //console.log(facturas);
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
                    data_to_insert.fechaFin = t
                    data_to_insert.idents = tbai_array;
                    data_to_insert.agrupacion = zlib.gzipSync(agrupacion, GZIP_PARAMS).toString("base64");
                    //await insert_agrupadas_mongo([data_to_insert]);

                    await new DayAgrupadas().collection.insertMany([data_to_insert], { ordered: false }, (err, docs) => {
                        if (err) { console.log("Error en nif " + nif_array[i] + "ERROR --> " + err); }
                    });

                } else {
                    //console.log("Error al transformar las facturas del nif --> " + nif_array[i]);

                }
            }
            //console.log("Insertado NIF --> " + nif_array[i]);
        }

        resolve("OK");
    });
}

function agruparTrimes() {
    return new Promise(async resolve => {
        var nif_array = companies_nif_list.slice(0, 3000).map(n => n[0]);
        for (var i = 0; i < nif_array.length; i++) {
            let facturas = await Factura.find({
                nif: nif_array[i]
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
                data_to_insert.fechaInicio = new Date("2021-01-04T00:00:00");
                data_to_insert.fechaFin = new Date("2021-03-28T00:00:00")
                data_to_insert.idents = tbai_array;
                data_to_insert.agrupacion = zlib.gzipSync(agrupacion, GZIP_PARAMS).toString("base64");

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
                //var comprimir_mongo = [];
                if (numParticiones == 1) {
                    insert_array.push(data_to_insert);
                    //comprimir_mongo.push(compresion_fin - compresion_start);
                } else {
                    //console.log(numParticiones);
                    for (var j = 0; j < numParticiones; j++) {
                        var agrupacion_mongo = "";
                        var tbai_part_list = [];
                        //console.log( Math.round(((j * facturas.length) / numParticiones)) + 1);
                        for (var k = Math.round(((j * facturas.length) / numParticiones)); k < Math.round((j + 1) * facturas.length) / numParticiones; k++) {
                            //let factura = fs.readFileSync(FACTURAS_AGRUPADAS_PATH + "grupo_" + num_agrupadas + "_" + k + ".xml").toString();
                            //let agrupacion = facturas_array.slice((k * i) / numParticiones, ((k + 1) * i) / numParticiones).join('');
                            //console.log(k);
                            let factura = zlib.gunzipSync(Buffer.from(facturas[k].xml, "base64"), GZIP_PARAMS).toString();
                            agrupacion_mongo += factura;
                            tbai_part_list.push(DATA.getIdentTBAI(factura));
                        }
                        //console.log("Insert --> " + j);
                        let new_data_to_insert = {};
                        new_data_to_insert.nif = nif_array[i];
                        new_data_to_insert.fechaInicio = new Date("2021-01-04T00:00:00");
                        new_data_to_insert.fechaFin = new Date("2021-03-28T00:00:00")
                        new_data_to_insert.idents = tbai_part_list;
                        new_data_to_insert.agrupacion = zlib.gzipSync(agrupacion_mongo, GZIP_PARAMS).toString("base64");

                        insert_array.push(new_data_to_insert);
                        //console.log("fin");
                    }
                }

                await new TriMesAgrupadas().collection.insertMany(insert_array, { ordered: false }, (err, docs) => {
                    if (err) { console.log("Error en nif " + nif_array[i] + "ERROR --> " + err); }
                });

            } else {
                console.log("Error al transformar las facturas del nif --> " + nif_array[i]);
            }
            //console.log("Insertado NIF --> " + nif_array[i]);
        }

        //res.status(200).send("OK");
        resolve("OK");
    });
}

function findAgrupadasCassandra(tbai_id, client) {
    return new Promise((resolve, reject) => {
        const query_gr = "select fecha_fin, tbai_id_list, agrupacion from facturas_agrupadas where nif=? and fecha_inicio <= ?";
        const params_gr = [
            tbai_id.split("-")[1],
            moment(tbai_id.split("-")[2], "DDMMYY").format("YYYY-MM-DD")
        ];
        var busqueda_bd_start = performance.now();
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
        }).catch(err => {
            reject(err);
        });
    });
}

function findByIdCassandra(tbai_id, client) {
    return new Promise((resolve, reject) => {
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
        }).catch(err => { reject(err) });

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

function createEstadisticaDiaAgrupadas(nif) {
    return new Promise(async (resolve, reject) => {
        try {
            var start = performance.now();
            //console.log("Dentro del try");
            if (fs.existsSync("./estadisticas/2021-03-19_2021-03-19_global.txt")) {//Si existe la de un nif entonces existe la global
                //console.log("Existe fichero?");
                var global_dia_estadistica = fs.readFileSync("./estadisticas/2021-03-19_2021-03-19_global.txt").toString().split("\n");
                if (global_dia_estadistica.map(l => l.split(" // ")[0]).filter(n => n == `dia_${nif}`).length == 0) {//No existe la estadistica sobre ese nif
                    //console.log("No existe estadistica sobre el nif");
                    var date = new Date("2021-03-19T00:00:00");
                    var buscar_datos_start = performance.now();
                    TriMesAgrupadas.find({
                        nif: nif,
                        fechaInicio: { $lte: date },
                        fechaFin: { $gte: date }
                    }, "idents agrupacion", (err, query_dia_result) => {
                        var buscar_datos_fin = performance.now() - buscar_datos_start;
                        //console.log(query_dia_result[0].idents);
                        var json = { "00:00:00": { suma: 0, cantidad: 0, avg: 0 }, "01:00:00": { suma: 0, cantidad: 0, avg: 0 }, "02:00:00": { suma: 0, cantidad: 0, avg: 0 }, "03:00:00": { suma: 0, cantidad: 0, avg: 0 }, "04:00:00": { suma: 0, cantidad: 0, avg: 0 }, "05:00:00": { suma: 0, cantidad: 0, avg: 0 }, "06:00:00": { suma: 0, cantidad: 0, avg: 0 }, "07:00:00": { suma: 0, cantidad: 0, avg: 0 }, "08:00:00": { suma: 0, cantidad: 0, avg: 0 }, "09:00:00": { suma: 0, cantidad: 0, avg: 0 }, "10:00:00": { suma: 0, cantidad: 0, avg: 0 }, "11:00:00": { suma: 0, cantidad: 0, avg: 0 }, "12:00:00": { suma: 0, cantidad: 0, avg: 0 }, "13:00:00": { suma: 0, cantidad: 0, avg: 0 }, "14:00:00": { suma: 0, cantidad: 0, avg: 0 }, "15:00:00": { suma: 0, cantidad: 0, avg: 0 }, "16:00:00": { suma: 0, cantidad: 0, avg: 0 }, "17:00:00": { suma: 0, cantidad: 0, avg: 0 }, "18:00:00": { suma: 0, cantidad: 0, avg: 0 }, "19:00:00": { suma: 0, cantidad: 0, avg: 0 }, "20:00:00": { suma: 0, cantidad: 0, avg: 0 }, "21:00:00": { suma: 0, cantidad: 0, avg: 0 }, "22:00:00": { suma: 0, cantidad: 0, avg: 0 }, "23:00:00": { suma: 0, cantidad: 0, avg: 0 } };
                        var tratar_facturas_start = performance.now();
                        var descomprimir_total = 0;
                        for (var i = 0; i < query_dia_result.length; i++) {
                            var grupo = query_dia_result[i];
                            let descomp_inicio = performance.now();
                            var grupo_descomp = zlib.gunzipSync(Buffer.from(grupo.agrupacion, "base64"), GZIP_PARAMS).toString();
                            descomprimir_total += performance.now() - descomp_inicio;
                            var facturas_array = grupo_descomp.split(/(?=\<\?xml version="1\.0" encoding="utf-8"\?\>)/);
                            grupo.idents.forEach((ident, index) => {
                                if (moment(ident.split("-")[2], "DDMMYY").toDate().getTime() == date.getTime()) {
                                    let factura = facturas_array[index];
                                    let hora = DATA.getHoraExpedionFactura(factura);
                                    let cantidad = DATA.getImporteTotalFactura(factura);

                                    let hora_split = hora.split(":")[0];

                                    json[hora_split + ':00:00'].suma += cantidad;
                                    json[hora_split + ':00:00'].cantidad++;
                                    json[hora_split + ':00:00'].avg = json[hora_split + ':00:00'].suma / json[hora_split + ':00:00'].cantidad;
                                }
                            });
                        }
                        var tratar_facturas_fin = performance.now() - tratar_facturas_start;
                        var dia_nif = [];
                        //console.log(json);
                        for (var i = 0; i < 24; i++) {
                            let hora = i.toString().padStart(2, 0);
                            dia_nif.push(json[hora + ':00:00'].avg);
                        }
                        fs.appendFileSync('./files/estadisticas_diarias_stats_nif.txt', `NIF;BuscarDatosGlobal;DescomprimirFacturas;TratarFacturas;CalcularEstadisticaGlobal\n${nif};${buscar_datos_fin};${descomprimir_total};${tratar_facturas_fin - descomprimir_total}\n`);
                        fs.appendFileSync('./estadisticas/2021-03-19_2021-03-19_global.txt', `dia_${nif} // [${dia_nif.toString()}]\n`);
                        resolve("OK");

                    });
                } else {
                    resolve("OK");
                }//end if
            } else {//No existe la estadistica global ni la del nif, asi que tengo que crearlas
                //console.log("No existe fichero");
                var buscar_datos_fin = 0;
                var nif_list = companies_nif_list.slice(0, 3000).map(c => c[0]);
                var date = new Date("2021-03-19T00:00:00");
                var descomprimir_total = 0;
                var tratar_facturas_fin = 0;

                for (var k = 0; k < nif_list.length; k++) {
                    let nif = nif_list[k];
                    console.log(nif_list.length - (k + 1));
                    var buscar_datos_start = performance.now();
                    /*let query_dia_result = await MesAgrupadas.aggregate([
                        {
                            $match: {
                                nif: nif,
                                fechaInicio: { $lte: date },
                                fechaFin: { $gte: date }
                            }
                        },
                        {
                            $project: {
                                _id: "null",
                                nif: "$nif",
                                idents: "$idents",
                                agrupacion: "$agrupacion"
                            }
                        }
                    ]).allowDiskUse();//.exec((err, query_dia_result) => {*/

                    let query_dia_result = await DayAgrupadas.find({
                        nif: nif,
                        fechaInicio: { $lte: date },
                        fechaFin: { $gte: date }
                    }, "nif idents agrupacion");
                    //if (err) throw err;
                    buscar_datos_fin += performance.now() - buscar_datos_start;
                    const dia_labels = ["00:00:00", "01:00:00", "02:00:00", "03:00:00", "04:00:00", "05:00:00", "06:00:00", "07:00:00", "08:00:00", "09:00:00", "10:00:00", "11:00:00", "12:00:00", "13:00:00", "14:00:00", "15:00:00", "16:00:00", "17:00:00", "18:00:00", "19:00:00", "20:00:00", "21:00:00", "22:00:00", "23:00:00",];
                    var dia_nif = [];
                    var dia_sector = [];

                    //var descomprimir_total = 0;
                    var json = { "00:00:00": { suma: 0, cantidad: 0, avg: 0 }, "01:00:00": { suma: 0, cantidad: 0, avg: 0 }, "02:00:00": { suma: 0, cantidad: 0, avg: 0 }, "03:00:00": { suma: 0, cantidad: 0, avg: 0 }, "04:00:00": { suma: 0, cantidad: 0, avg: 0 }, "05:00:00": { suma: 0, cantidad: 0, avg: 0 }, "06:00:00": { suma: 0, cantidad: 0, avg: 0 }, "07:00:00": { suma: 0, cantidad: 0, avg: 0 }, "08:00:00": { suma: 0, cantidad: 0, avg: 0 }, "09:00:00": { suma: 0, cantidad: 0, avg: 0 }, "10:00:00": { suma: 0, cantidad: 0, avg: 0 }, "11:00:00": { suma: 0, cantidad: 0, avg: 0 }, "12:00:00": { suma: 0, cantidad: 0, avg: 0 }, "13:00:00": { suma: 0, cantidad: 0, avg: 0 }, "14:00:00": { suma: 0, cantidad: 0, avg: 0 }, "15:00:00": { suma: 0, cantidad: 0, avg: 0 }, "16:00:00": { suma: 0, cantidad: 0, avg: 0 }, "17:00:00": { suma: 0, cantidad: 0, avg: 0 }, "18:00:00": { suma: 0, cantidad: 0, avg: 0 }, "19:00:00": { suma: 0, cantidad: 0, avg: 0 }, "20:00:00": { suma: 0, cantidad: 0, avg: 0 }, "21:00:00": { suma: 0, cantidad: 0, avg: 0 }, "22:00:00": { suma: 0, cantidad: 0, avg: 0 }, "23:00:00": { suma: 0, cantidad: 0, avg: 0 } };
                    var json_nif = { "00:00:00": { suma: 0, cantidad: 0, avg: 0 }, "01:00:00": { suma: 0, cantidad: 0, avg: 0 }, "02:00:00": { suma: 0, cantidad: 0, avg: 0 }, "03:00:00": { suma: 0, cantidad: 0, avg: 0 }, "04:00:00": { suma: 0, cantidad: 0, avg: 0 }, "05:00:00": { suma: 0, cantidad: 0, avg: 0 }, "06:00:00": { suma: 0, cantidad: 0, avg: 0 }, "07:00:00": { suma: 0, cantidad: 0, avg: 0 }, "08:00:00": { suma: 0, cantidad: 0, avg: 0 }, "09:00:00": { suma: 0, cantidad: 0, avg: 0 }, "10:00:00": { suma: 0, cantidad: 0, avg: 0 }, "11:00:00": { suma: 0, cantidad: 0, avg: 0 }, "12:00:00": { suma: 0, cantidad: 0, avg: 0 }, "13:00:00": { suma: 0, cantidad: 0, avg: 0 }, "14:00:00": { suma: 0, cantidad: 0, avg: 0 }, "15:00:00": { suma: 0, cantidad: 0, avg: 0 }, "16:00:00": { suma: 0, cantidad: 0, avg: 0 }, "17:00:00": { suma: 0, cantidad: 0, avg: 0 }, "18:00:00": { suma: 0, cantidad: 0, avg: 0 }, "19:00:00": { suma: 0, cantidad: 0, avg: 0 }, "20:00:00": { suma: 0, cantidad: 0, avg: 0 }, "21:00:00": { suma: 0, cantidad: 0, avg: 0 }, "22:00:00": { suma: 0, cantidad: 0, avg: 0 }, "23:00:00": { suma: 0, cantidad: 0, avg: 0 } };

                    var tratar_facturas_start = performance.now();

                    for (var i = 0; i < query_dia_result.length; i++) {
                        var grupo = query_dia_result[i];
                        let descomp_inicio = performance.now();
                        var grupo_descomp = zlib.gunzipSync(Buffer.from(grupo.agrupacion, "base64"), GZIP_PARAMS).toString();
                        descomprimir_total += performance.now() - descomp_inicio;
                        var facturas_array = grupo_descomp.split(/(?=\<\?xml version="1\.0" encoding="utf-8"\?\>)/);
                        grupo.idents.forEach((ident, index) => {
                            if (moment(ident.split("-")[2], "DDMMYY").toDate().getTime() == date.getTime()) {
                                let factura = facturas_array[index];
                                let hora = DATA.getHoraExpedionFactura(factura);
                                let cantidad = DATA.getImporteTotalFactura(factura);

                                let hora_split = hora.split(":")[0];

                                json[hora_split + ':00:00'].suma += cantidad;
                                json[hora_split + ':00:00'].cantidad++;
                                json[hora_split + ':00:00'].avg = json[hora_split + ':00:00'].suma / json[hora_split + ':00:00'].cantidad;
                                if (grupo.nif == nif) {
                                    json_nif[hora_split + ':00:00'].suma += cantidad;
                                    json_nif[hora_split + ':00:00'].cantidad++;
                                    json_nif[hora_split + ':00:00'].avg = json[hora_split + ':00:00'].suma / json[hora_split + ':00:00'].cantidad;
                                }
                            }
                        });
                    }
                    tratar_facturas_fin += performance.now() - tratar_facturas_start;

                    for (var i = 0; i < 24; i++) {
                        let hora = i.toString().padStart(2, 0);
                        dia_nif.push(json_nif[hora + ':00:00'].avg);
                        dia_sector.push(json[hora + ':00:00'].avg)
                    }


                    //});
                }

                fs.appendFileSync('./files/estadisticas_diarias_stats.txt', `BuscarDatosGlobal;DescomprimirFacturas;TratarFacturas;CalcularEstadisticaGlobal\n${buscar_datos_fin};${descomprimir_total};${tratar_facturas_fin - descomprimir_total}\n`);
                fs.writeFileSync('./estadisticas/2021-03-19_2021-03-19_global.txt', `dia_labels // ["${dia_labels.join('","')}"]\ndia_sector // [${dia_sector.toString()}]\ndia_${nif} // [${dia_nif.toString()}]\n`);
                resolve("OK");

            }//end if
        } catch (err) {

            //console.log(err);
            reject(err);
        }
    });
}


function createEstadisticaSemanalAgrupadas(nif) {
    return new Promise(async (resolve, reject) => {
        console.log("Estadistica Semanal");

        try {
            var start = performance.now();
            //console.log("Dentro del try");
            if (fs.existsSync("./estadisticas/2021-03-15_2021-03-21_global_123.txt")) {//Si existe la de un nif entonces existe la global
                //console.log("Existe fichero?");
                var global_semana_estadistica = fs.readFileSync("./estadisticas/2021-03-15_2021-03-21_global.txt").toString().split("\n");
                if (global_semana_estadistica.map(l => l.split(" // ")[0]).filter(n => n == `triMes_${nif}`).length == 0) {//No existe la estadistica sobre ese nif
                    //console.log("No existe estadistica sobre el nif");
                    var date_start = new Date("2021-03-15T00:00:00");
                    var date_fin = new Date("2021-03-21T00:00:00");
                    var buscar_datos_start = performance.now();
                    /*TriMesAgrupadas.find({
                        nif: nif,
                        fechaInicio: { $lte: date_start },
                        fechaFin: { $gte: date_fin }
                    }, "idents agrupacion", (err, query_semana_result) => {*/
                    MesAgrupadas.aggregate([
                        {
                            $match: {
                                nif: nif,
                                fechaInicio: { $gte: date_start }
                            }
                        },
                        {
                            $match: {
                                fechaFin: { $lte: date_fin }
                            }
                        },
                        {
                            $project: {
                                idents: 1,
                                agrupacion: 1
                            }
                        }
                    ], (err, query_semana_result) => {

                        var buscar_datos_fin = performance.now() - buscar_datos_start;
                        var semana_nif = [];
                        var json = {};
                        var descomprimir_total = 0;
                        var tratar_facturas_start = performance.now();
                        for (var i = 0; i < query_semana_result.length; i++) {
                            var grupo = query_semana_result[i];
                            var descomprimir_start = performance.now();
                            var grupo_descomp = zlib.gunzipSync(Buffer.from(grupo.agrupacion, "base64"), GZIP_PARAMS).toString();
                            descomprimir_total += performance.now() - descomprimir_start;
                            var facturas_array = grupo_descomp.split(/(?=\<\?xml version="1\.0" encoding="utf-8"\?\>)/);
                            grupo.idents.forEach((ident, index) => {
                                let fecha = moment(ident.split("-")[2], "DDMMYY").toDate();
                                if (fecha.getTime() >= date_start.getTime() && fecha.getTime() <= date_fin.getTime()) {
                                    let factura = facturas_array[index];
                                    let day = moment(fecha).format("DD-MM");
                                    if (!json.hasOwnProperty(day)) {
                                        json[day] = {
                                            suma: 0,
                                            cantidad: 0,
                                            avg: 0
                                        }
                                    }
                                    json[day].suma += DATA.getImporteTotalFactura(factura);
                                    json[day].cantidad++;
                                    json[day].avg = json[day].suma / json[day].cantidad
                                }
                            });
                        }
                        //console.log(json);
                        var tratar_facturas_fin = performance.now() - tratar_facturas_start;
                        for (var i = moment(date_start).toDate(); i <= moment(date_fin).toDate(); i = moment(i).add(1, 'days')) {
                            let day = moment(i).format("DD-MM");
                            if (json.hasOwnProperty(day)) {
                                semana_nif.push(json[day].avg);
                            } else {
                                semana_nif.push(0);
                            }
                        }
                        fs.appendFileSync('./files/estadisticas_triMes_stats_nif.txt', `NIF;BuscarDatosGlobal;DescomprimirFacturas;TratarFacturas\n${nif};${buscar_datos_fin};${descomprimir_total};${tratar_facturas_fin - descomprimir_total}\n`);
                        fs.appendFileSync('./estadisticas/2021-01-01_2021-03-28_global.txt', `triMes_${nif} // [${semana_nif.toString()}]\n`);
                        //console.log(performance.now() - start);
                        resolve("OK");
                    }).allowDiskUse();
                } else {
                    resolve("OK");
                }//end if
            } else {//No existe la estadistica global ni la del nif, asi que tengo que crearlas
                //console.log("No existe fichero");
                var tiempo_start = performance.now();
                var nif_list = companies_nif_list.slice(0, 3000).map(c => c[0]);
                const date_start = new Date("2021-03-19T00:00:00");
                const date_fin = new Date("2021-03-19T00:00:00");
                var descomprimir_total = 0;
                var buscar_datos_fin = 0;
                var tratar_facturas_fin = 0;
                /* Factura.find({
                     nif: {
                         $in: nif_list
                     },
                     fecha: {
                         $gte: new Date("2021-03-15T00:00:00"),
                         $lte: new Date("2021-03-21T23:59:59")
                     }
                 }, "nif fecha cantidad", (err, query_semana_result) => {*/
                    console.log(nif_list.length);
                for(var k = 0 ; k < nif_list.length; k++){
               // nif_list.forEach(async (nif, index) => {
                    console.log(k);

                    let nif = nif_list[k];
                    var buscar_datos_start = performance.now();
                    var query_semana_result = await MesAgrupadas.find(
                        {
                            nif: nif,
                            fechaInicio: {
                                $lte: date_start
                            },
                            fechaFin: {
                                $gte: date_fin
                            }

                        },
                        "nif idents agrupacion");
                    /*var query_semana_result = await MesAgrupadas.aggregate([
                        {
                            $match : {
                                nif: nif,
                                fechaInicio: {
                                    $gte: date_start
                                }
                            }
                        },
                        {
                        $match : {
                            fechaFin : {
                                $lte : date_fin
                            }
                        }
                    },
                    {
                        $project : {
                            nif:1,
                            idents:1,
                            agrupacion:1
                        }
                    }
                    ]);*/
                    //, (err, query_semana_result) => {
                    if (query_semana_result != null && query_semana_result.length > 0) {
                        //console.log(query_semana_result[0].nif);
                        buscar_datos_fin += performance.now() - buscar_datos_start;
                        //descomprimir_total = performance.now();
                        var json_sector = {};
                        var json_nif = {};
                        var tratar_facturas_start = performance.now();
                        for (var i = 0; i < query_semana_result.length; i++) {
                            let descomprimir_start = performance.now();
                            let grupo_descomp = zlib.gunzipSync(Buffer.from(query_semana_result[i].agrupacion, "base64"), GZIP_PARAMS).toString();
                            descomprimir_total += performance.now() - descomprimir_start;
                            var facturas_array = grupo_descomp.split(/(?=\<\?xml version="1\.0" encoding="utf-8"\?\>)/);
                            //facturas_array.forEach((factura, index) => {
                            for(var l = 0 ; l < facturas_array.length; l++){
                               if(query_semana_result[i].idents.length > 0){
                                let factura = facturas_array[l];
                                let fecha = moment(query_semana_result[i].idents[l].split("-")[2], "DDMMYY").toDate();
                                let dia = moment(DATA.getFechaExp(factura), "DD-MM-YYYY").format("DD");
                                let importe = DATA.getImporteTotalFactura(factura);

                                if (!json_sector.hasOwnProperty(dia)) {
                                    json_sector[dia] = {
                                        importe: 0,
                                        cantidad: 0,
                                        avg: 0
                                    }
                                }

                                json_sector[dia].importe += importe;
                                json_sector[dia].cantidad++;
                                json_sector[dia].avg = json_sector[dia].importe / json_sector[dia].cantidad;


                                if (query_semana_result[i].nif == nif) {
                                    if (!json_nif.hasOwnProperty(dia)) {
                                        json_nif[dia] = {
                                            importe: 0,
                                            cantidad: 0,
                                            avg: 0
                                        }
                                    }

                                    json_nif[dia].importe += importe;
                                    json_nif[dia].cantidad++;
                                    json_nif[dia].avg = json_sector[dia].importe / json_sector[dia].cantidad;
                                }
                               }
                            }
                            //console.log(json_sector);
                        }//end for
                        tratar_facturas_fin += performance.now() - tratar_facturas_start;
                        var semana_labels = [];
                        var semana_nif = [];
                        var semana_sector = [];

                        for (var i = moment(date_start).toDate(); i <= moment(date_fin).toDate(); i = moment(i).add(1, 'days')) {
                            let day = moment(i).format("DD");
                            if (json_sector.hasOwnProperty(day)) {
                                semana_sector.push(json_sector[day].avg);
                            } else {
                                semana_sector.push(0);
                            }

                            if (json_nif.hasOwnProperty(day)) {
                                semana_nif.push(json_sector[day].avg);
                            } else {
                                semana_nif.push(0);
                            }
                        }
                    }
                    //});


                    //}).allowDiskUse();
                }//end for

                fs.appendFileSync('./files/estadisticas_mes_stats_sector.txt', `BuscarDatosGlobal;DescomprimirFacturas;TratarFacturas\n${buscar_datos_fin};${descomprimir_total};${tratar_facturas_fin - descomprimir_total}\n`);
                //fs.appendFileSync('./estadisticas/2021-03-15_2021-03-21_global.txt', `semana_labels // ["${semana_labels.join('","')}"]\nsemana_sector // [${semana_sector.toString()}]\nsemana_${nif} // [${semana_nif.toString()}]\n`);
                //resolve("OK");
            }//end if
        } catch (err) {

            //console.log(err);
            reject(err);
        }
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
                        //console.log(query_dia_result.length);
                        var tratar_facturas_start = performance.now();
                        var descomprimir_total = 0;
                        var json = { "00:00:00": { suma: 0, cantidad: 0, avg: 0 }, "01:00:00": { suma: 0, cantidad: 0, avg: 0 }, "02:00:00": { suma: 0, cantidad: 0, avg: 0 }, "03:00:00": { suma: 0, cantidad: 0, avg: 0 }, "04:00:00": { suma: 0, cantidad: 0, avg: 0 }, "05:00:00": { suma: 0, cantidad: 0, avg: 0 }, "06:00:00": { suma: 0, cantidad: 0, avg: 0 }, "07:00:00": { suma: 0, cantidad: 0, avg: 0 }, "08:00:00": { suma: 0, cantidad: 0, avg: 0 }, "09:00:00": { suma: 0, cantidad: 0, avg: 0 }, "10:00:00": { suma: 0, cantidad: 0, avg: 0 }, "11:00:00": { suma: 0, cantidad: 0, avg: 0 }, "12:00:00": { suma: 0, cantidad: 0, avg: 0 }, "13:00:00": { suma: 0, cantidad: 0, avg: 0 }, "14:00:00": { suma: 0, cantidad: 0, avg: 0 }, "15:00:00": { suma: 0, cantidad: 0, avg: 0 }, "16:00:00": { suma: 0, cantidad: 0, avg: 0 }, "17:00:00": { suma: 0, cantidad: 0, avg: 0 }, "18:00:00": { suma: 0, cantidad: 0, avg: 0 }, "19:00:00": { suma: 0, cantidad: 0, avg: 0 }, "20:00:00": { suma: 0, cantidad: 0, avg: 0 }, "21:00:00": { suma: 0, cantidad: 0, avg: 0 }, "22:00:00": { suma: 0, cantidad: 0, avg: 0 }, "23:00:00": { suma: 0, cantidad: 0, avg: 0 } };
                        query_dia_result.forEach((factura_com, index) => {
                            var descomprimir_start = performance.now();
                            let factura_descomp = zlib.gunzipSync(Buffer.from(factura_com.xml, "base64"), GZIP_PARAMS).toString();
                            descomprimir_total += (performance.now() - descomprimir_start);
                            let hora = DATA.getHoraExpedionFactura(factura_descomp);
                            let cantidad = factura_com.cantidad;

                            let hora_split = hora.split(":")[0];

                            json[hora_split + ':00:00'].suma += cantidad;
                            json[hora_split + ':00:00'].cantidad++;
                            json[hora_split + ':00:00'].avg = json[hora_split + ':00:00'].suma / json[hora_split + ':00:00'].cantidad;
                        });
                        var tratar_facturas_fin = performance.now() - tratar_facturas_start;
                        var dia_nif = [];
                        for (var i = 0; i < 24; i++) {
                            let hora = i.toString().padStart(2, 0);
                            dia_nif.push(json[hora + ':00:00'].avg);
                        }
                        fs.appendFileSync('./files/estadisticas_diarias_stats_nif.txt', `NIF;BuscarDatosGlobal;DescomprimirFacturas;TratarFacturas;CalcularEstadisticaGlobal\n${nif};${buscar_datos_fin};${descomprimir_total};${tratar_facturas_fin - descomprimir_total}\n`);
                        fs.appendFileSync('./estadisticas/2021-03-19_2021-03-19_global.txt', `dia_${nif} // [${dia_nif.toString()}]\n`);
                        resolve("OK");

                    });
                } else {
                    resolve("OK");
                }//end if
            } else {//No existe la estadistica global ni la del nif, asi que tengo que crearlas
                //console.log("No existe fichero");

                var nif_list = companies_nif_list.slice(0, 3000).map(c => c[0]);

                //console.log("Busqueda start");
                var buscar_datos_start = performance.now();
                Factura.aggregate([
                    {
                        $match: {
                            nif: {
                                $in: nif_list
                            },
                            fecha: new Date("2021-03-19T00:00:00")
                        }
                    },
                    {
                        $project: {
                            _id: "$_id",
                            nif: "$nif",
                            fecha: "%fecha",
                            cantidad: "$cantidad",
                            xml: "$xml"
                        }
                    }
                ]).allowDiskUse().exec((err, query_dia_result) => {
                    if (err) throw err;
                    var buscar_datos_fin = performance.now() - buscar_datos_start;
                    //console.log("End Busqueda datos");
                    //console.log(query_dia_result.length);
                    const dia_labels = ["00:00:00", "01:00:00", "02:00:00", "03:00:00", "04:00:00", "05:00:00", "06:00:00", "07:00:00", "08:00:00", "09:00:00", "10:00:00", "11:00:00", "12:00:00", "13:00:00", "14:00:00", "15:00:00", "16:00:00", "17:00:00", "18:00:00", "19:00:00", "20:00:00", "21:00:00", "22:00:00", "23:00:00",];
                    var dia_nif = [];
                    var dia_sector = [];

                    var descomprimir_total = 0;
                    var json = { "00:00:00": { suma: 0, cantidad: 0, avg: 0 }, "01:00:00": { suma: 0, cantidad: 0, avg: 0 }, "02:00:00": { suma: 0, cantidad: 0, avg: 0 }, "03:00:00": { suma: 0, cantidad: 0, avg: 0 }, "04:00:00": { suma: 0, cantidad: 0, avg: 0 }, "05:00:00": { suma: 0, cantidad: 0, avg: 0 }, "06:00:00": { suma: 0, cantidad: 0, avg: 0 }, "07:00:00": { suma: 0, cantidad: 0, avg: 0 }, "08:00:00": { suma: 0, cantidad: 0, avg: 0 }, "09:00:00": { suma: 0, cantidad: 0, avg: 0 }, "10:00:00": { suma: 0, cantidad: 0, avg: 0 }, "11:00:00": { suma: 0, cantidad: 0, avg: 0 }, "12:00:00": { suma: 0, cantidad: 0, avg: 0 }, "13:00:00": { suma: 0, cantidad: 0, avg: 0 }, "14:00:00": { suma: 0, cantidad: 0, avg: 0 }, "15:00:00": { suma: 0, cantidad: 0, avg: 0 }, "16:00:00": { suma: 0, cantidad: 0, avg: 0 }, "17:00:00": { suma: 0, cantidad: 0, avg: 0 }, "18:00:00": { suma: 0, cantidad: 0, avg: 0 }, "19:00:00": { suma: 0, cantidad: 0, avg: 0 }, "20:00:00": { suma: 0, cantidad: 0, avg: 0 }, "21:00:00": { suma: 0, cantidad: 0, avg: 0 }, "22:00:00": { suma: 0, cantidad: 0, avg: 0 }, "23:00:00": { suma: 0, cantidad: 0, avg: 0 } };
                    var json_nif = { "00:00:00": { suma: 0, cantidad: 0, avg: 0 }, "01:00:00": { suma: 0, cantidad: 0, avg: 0 }, "02:00:00": { suma: 0, cantidad: 0, avg: 0 }, "03:00:00": { suma: 0, cantidad: 0, avg: 0 }, "04:00:00": { suma: 0, cantidad: 0, avg: 0 }, "05:00:00": { suma: 0, cantidad: 0, avg: 0 }, "06:00:00": { suma: 0, cantidad: 0, avg: 0 }, "07:00:00": { suma: 0, cantidad: 0, avg: 0 }, "08:00:00": { suma: 0, cantidad: 0, avg: 0 }, "09:00:00": { suma: 0, cantidad: 0, avg: 0 }, "10:00:00": { suma: 0, cantidad: 0, avg: 0 }, "11:00:00": { suma: 0, cantidad: 0, avg: 0 }, "12:00:00": { suma: 0, cantidad: 0, avg: 0 }, "13:00:00": { suma: 0, cantidad: 0, avg: 0 }, "14:00:00": { suma: 0, cantidad: 0, avg: 0 }, "15:00:00": { suma: 0, cantidad: 0, avg: 0 }, "16:00:00": { suma: 0, cantidad: 0, avg: 0 }, "17:00:00": { suma: 0, cantidad: 0, avg: 0 }, "18:00:00": { suma: 0, cantidad: 0, avg: 0 }, "19:00:00": { suma: 0, cantidad: 0, avg: 0 }, "20:00:00": { suma: 0, cantidad: 0, avg: 0 }, "21:00:00": { suma: 0, cantidad: 0, avg: 0 }, "22:00:00": { suma: 0, cantidad: 0, avg: 0 }, "23:00:00": { suma: 0, cantidad: 0, avg: 0 } };

                    var tratar_facturas_start = performance.now();
                    query_dia_result.forEach((factura_com, index) => {
                        var descomprimir_start = performance.now();
                        let factura_descomp = zlib.gunzipSync(Buffer.from(factura_com.xml, "base64"), GZIP_PARAMS).toString();
                        descomprimir_total += (performance.now() - descomprimir_start);
                        let hora = DATA.getHoraExpedionFactura(factura_descomp);
                        let cantidad = factura_com.cantidad;

                        let hora_split = hora.split(":")[0];

                        json[hora_split + ':00:00'].suma += cantidad;
                        json[hora_split + ':00:00'].cantidad++;
                        json[hora_split + ':00:00'].avg = json[hora_split + ':00:00'].suma / json[hora_split + ':00:00'].cantidad;
                        if (factura_com.nif == nif) {
                            json_nif[hora_split + ':00:00'].suma += cantidad;
                            json_nif[hora_split + ':00:00'].cantidad++;
                            json_nif[hora_split + ':00:00'].avg = json[hora_split + ':00:00'].suma / json[hora_split + ':00:00'].cantidad;
                        }

                    });
                    console.log("End tratar facturas");
                    var tratar_facturas_fin = performance.now() - tratar_facturas_start;

                    for (var i = 0; i < 24; i++) {
                        let hora = i.toString().padStart(2, 0);
                        dia_nif.push(json_nif[hora + ':00:00'].avg);
                        dia_sector.push(json[hora + ':00:00'].avg)
                    }

                    fs.appendFileSync('./files/estadisticas_diarias_stats.txt', `BuscarDatosGlobal;DescomprimirFacturas;TratarFacturas;CalcularEstadisticaGlobal;BuscarDatosNIF\n${buscar_datos_fin};${descomprimir_total};${tratar_facturas_fin - descomprimir_total}\n`);
                    fs.writeFileSync('./estadisticas/2021-03-19_2021-03-19_global.txt', `dia_labels // ["${dia_labels.join('","')}"]\ndia_sector // [${dia_sector.toString()}]\ndia_${nif} // [${dia_nif.toString()}]\n`);
                    resolve("OK");
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
                    var buscar_datos_start = performance.now();
                    Factura.find({
                        nif: nif,
                        fecha: {
                            $gte: new Date("2021-03-15T00:00:00"),
                            $lte: new Date("2021-03-21T23:59:59")
                        }
                    }, "nif fecha cantidad", (err, query_semana_result) => {
                        var buscar_datos_fin = performance.now() - buscar_datos_start;
                        var semana_nif = [];
                        //var query_dia_descomp = query_dia_result.map(f => zlib.gunzipSync(Buffer.from(f.xml, "base64"), GZIP_PARAMS).toString());
                        var tratar_facturas_start = performance.now();
                        for (var t = moment("2021-03-15").toDate(); t <= moment("2021-03-21").toDate(); t = moment(t).add(1, "days").toDate()) {
                            let t_aux = t;
                            let nif_array = query_semana_result.filter(f => moment(f.fecha).toDate() >= t && moment(f.fecha).toDate() < moment(t_aux).add(1, "days").toDate());
                            var nif_average = 0;
                            if (nif_array.length > 0) {
                                nif_average = nif_array.map(f => f.cantidad).reduce((a, b) => a + b, 0) / nif_array.length;
                            }
                            semana_nif.push(nif_average);
                        }
                        var tratar_facturas_fin = performance.now() - tratar_facturas_start;
                        fs.appendFileSync('./files/estadisticas_semanales_stats_nif.txt', `NIF;BuscarDatosGlobal;DescomprimirFacturas;TratarFacturas;CalcularEstadisticaGlobal\n${nif};${buscar_datos_fin};${0};${tratar_facturas_fin - 0}\n`);
                        fs.appendFileSync('./estadisticas/2021-03-15_2021-03-21_global.txt', `semana_${nif} // [${semana_nif.toString()}]\n`);
                        //console.log(performance.now() - start);
                        resolve("OK");
                    });
                } else {
                    resolve("OK");
                }//end if
            } else {//No existe la estadistica global ni la del nif, asi que tengo que crearlas
                //console.log("No existe fichero");
                var nif_list = companies_nif_list.slice(0, 3000).map(c => c[0]);
                const date_start = new Date("2021-03-15T00:00:00");
                const date_fin = new Date("2021-03-21T00:00:00");
                var buscar_datos_start = performance.now();
                Factura.aggregate([
                    {
                        $match: {
                            nif: {
                                $in: nif_list
                            },
                            fecha: {
                                $gte: date_start,
                                $lte: date_fin
                            }
                        }
                    },
                    {
                        $project: {
                            nif: 1,
                            fecha: 1,
                            cantidad: 1
                        }
                    }
                ], (err, query_semana_result) => {
                    var buscar_datos_fin = performance.now() - buscar_datos_start;
                    //console.log("Busqueda Fin");
                    var json_sector = {};
                    var json_nif = {};
                    var tratar_facturas_start = performance.now();
                    //for (var i = 0; i < query_semana_result.length; i++) {
                    //let descomprimir_start = performance.now();
                    //let grupo_descomp = zlib.gunzipSync(Buffer.from(query_semana_result[i].agrupacion, "base64"), GZIP_PARAMS).toString();
                    //descomprimir_total += performance.now() - descomprimir_start;
                    //var facturas_array = grupo_descomp.split(/(?=\<\?xml version="1\.0" encoding="utf-8"\?\>)/);
                    query_semana_result.forEach((factura, index) => {
                        //let dia = moment(DATA.getFechaExp(factura), "DD-MM-YYYY").format("DD");
                        //let importe = DATA.getImporteTotalFactura(factura);
                        let dia = moment(factura.fecha).format("DD");
                        let importe = factura.cantidad;

                        if (!json_sector.hasOwnProperty(dia)) {
                            json_sector[dia] = {
                                importe: 0,
                                cantidad: 0,
                                avg: 0
                            }
                        }

                        json_sector[dia].importe += importe;
                        json_sector[dia].cantidad++;
                        json_sector[dia].avg = json_sector[dia].importe / json_sector[dia].cantidad;


                        if (factura.nif == nif) {
                            if (!json_nif.hasOwnProperty(dia)) {
                                json_nif[dia] = {
                                    importe: 0,
                                    cantidad: 0,
                                    avg: 0
                                }
                            }

                            json_nif[dia].importe += importe;
                            json_nif[dia].cantidad++;
                            json_nif[dia].avg = json_nif[dia].importe / json_nif[dia].cantidad;
                        }
                    });
                    //}//end for
                    var tratar_facturas_fin = performance.now() - tratar_facturas_start;
                    var semana_labels = [];
                    var semana_nif = [];
                    var semana_sector = [];

                    //console.log(json_sector);

                    for (var i = moment(date_start).toDate(); i <= moment(date_fin).toDate(); i = moment(i).add(1, 'days')) {

                        let day = moment(i).format("DD");
                        semana_labels.push(moment(i).format("YYYY-MM-DD"));
                        if (json_sector.hasOwnProperty(day)) {
                            semana_sector.push(json_sector[day].avg);
                        } else {
                            semana_sector.push(0);
                        }

                        if (json_nif.hasOwnProperty(day)) {
                            semana_nif.push(json_nif[day].avg);
                        } else {
                            semana_nif.push(0);
                        }
                    }
                    fs.appendFileSync('./files/estadisticas_semana_stats_sector.txt', `BuscarDatosGlobal;DescomprimirFacturas;TratarFacturas\n${buscar_datos_fin};${0};${tratar_facturas_fin - 0}\n`);
                    fs.writeFileSync('./estadisticas/2021-03-15_2021-03-21_global.txt', `semana_labels // ["${semana_labels.join('","')}"]\nsemana_sector // [${semana_sector.toString()}]\nsemana_${nif} // [${semana_nif.toString()}]\n`);
                    resolve("OK");
                });

            }//end if
        } catch (err) {

            reject(err);
        }
    });
}

function createEstadisticaMensual(nif) {
    //await mongoose.connect(mongoUrl + "/" + dbName).then(() => { console.log("Conexión a MongoDB realizada correctamente") });
    return new Promise((resolve, reject) => {
        console.log("Estadistica Mensual");

        try {
            var start = performance.now();
            //console.log("Dentro del try");
            if (fs.existsSync("./estadisticas/2021-03-01_2021-03-28_global.txt")) {//Si existe la de un nif entonces existe la global
                //console.log("Existe fichero?");
                var global_mes_estadistica = fs.readFileSync("./estadisticas/2021-03-01_2021-03-28_global.txt").toString().split("\n");
                if (global_mes_estadistica.map(l => l.split(" // ")[0]).filter(n => n == `mes_${nif}`).length == 0) {//No existe la estadistica sobre ese nif
                    //console.log("No existe estadistica sobre el nif");
                    var date_ini = new Date("2021-03-01T00:00:00");
                    var date_fin = new Date("2021-03-28T00:00:00");
                    var buscar_datos_start = performance.now();
                    Factura.find({
                        nif: nif,
                        fecha: {
                            $gte: date_ini,
                            $lte: date_fin
                        }
                    }, "nif fecha cantidad", (err, query_mes_result) => {
                        var buscar_datos_fin = performance.now() - buscar_datos_start;
                        var mes_nif = [];
                        var json = {};
                        //var query_dia_descomp = query_dia_result.map(f => zlib.gunzipSync(Buffer.from(f.xml, "base64"), GZIP_PARAMS).toString());
                        var tratar_facturas_start = performance.now();

                        query_mes_result.forEach((factura, index) => {
                            let day = moment(factura.fecha).format("DD").padStart(2, 0);
                            if (!json.hasOwnProperty(day)) {
                                json[day] = {
                                    suma: 0,
                                    cantidad: 0,
                                    avg: 0
                                };
                            }

                            json[day].suma += factura.cantidad;
                            json[day].cantidad++;
                            json[day].avg = json[day].suma / json[day].cantidad;

                        });
                        //console.log(json);
                        var tratar_facturas_fin = performance.now() - tratar_facturas_start;

                        for (var i = Number(moment(date_ini).format("DD")); i <= Number(moment(date_fin).format("DD")); i++) {
                            let day = i.toString().padStart(2, 0);
                            if (json.hasOwnProperty(day)) {
                                mes_nif.push(json[day].avg);
                            } else {
                                mes_nif.push(0);
                            }
                        }

                        fs.appendFileSync('./files/estadisticas_mensuales_stats_nif.txt', `NIF;BuscarDatosGlobal;DescomprimirFacturas;TratarFacturas\n${nif};${buscar_datos_fin};${0};${tratar_facturas_fin - 0}\n`);
                        fs.appendFileSync('./estadisticas/2021-03-01_2021-03-28_global.txt', `mes_${nif} // [${mes_nif.toString()}]\n`);
                        //console.log(performance.now() - start);
                        resolve("OK");
                    });
                } else {
                    resolve("OK");
                }//end if
            } else {//No existe la estadistica global ni la del nif, asi que tengo que crearlas
                //console.log("No existe fichero");
                var nif_list = companies_nif_list.slice(0, 3000).map(c => c[0]);
                const date_start = new Date("2021-03-01T00:00:00");
                const date_fin = new Date("2021-03-28T00:00:00");
                var buscar_datos_start = performance.now();
                Factura.aggregate([
                    {
                        $match: {
                            nif: {
                                $in: nif_list
                            },
                            fecha: {
                                $gte: date_start,
                                $lte: date_fin
                            }
                        }
                    },
                    {
                        $project: {
                            nif: 1,
                            fecha: 1,
                            cantidad: 1
                        }
                    }
                ], (err, query_semana_result) => {
                    var buscar_datos_fin = performance.now() - buscar_datos_start;
                    //console.log("Busqueda Fin");
                    var json_sector = {};
                    var json_nif = {};
                    var tratar_facturas_start = performance.now();
                    //for (var i = 0; i < query_semana_result.length; i++) {
                    //let descomprimir_start = performance.now();
                    //let grupo_descomp = zlib.gunzipSync(Buffer.from(query_semana_result[i].agrupacion, "base64"), GZIP_PARAMS).toString();
                    //descomprimir_total += performance.now() - descomprimir_start;
                    //var facturas_array = grupo_descomp.split(/(?=\<\?xml version="1\.0" encoding="utf-8"\?\>)/);
                    query_semana_result.forEach((factura, index) => {
                        //let dia = moment(DATA.getFechaExp(factura), "DD-MM-YYYY").format("DD");
                        //let importe = DATA.getImporteTotalFactura(factura);
                        let dia = moment(factura.fecha).format("DD");
                        let importe = factura.cantidad;

                        if (!json_sector.hasOwnProperty(dia)) {
                            json_sector[dia] = {
                                importe: 0,
                                cantidad: 0,
                                avg: 0
                            }
                        }

                        json_sector[dia].importe += importe;
                        json_sector[dia].cantidad++;
                        json_sector[dia].avg = json_sector[dia].importe / json_sector[dia].cantidad;


                        if (factura.nif == nif) {
                            if (!json_nif.hasOwnProperty(dia)) {
                                json_nif[dia] = {
                                    importe: 0,
                                    cantidad: 0,
                                    avg: 0
                                }
                            }

                            json_nif[dia].importe += importe;
                            json_nif[dia].cantidad++;
                            json_nif[dia].avg = json_nif[dia].importe / json_nif[dia].cantidad;
                        }
                    });
                    //}//end for
                    var tratar_facturas_fin = performance.now() - tratar_facturas_start;
                    var mes_labels = [];
                    var mes_nif = [];
                    var mes_sector = [];

                    //console.log(json_sector);

                    for (var i = moment(date_start).toDate(); i <= moment(date_fin).toDate(); i = moment(i).add(1, 'days')) {

                        let day = moment(i).format("DD");
                        mes_labels.push(moment(i).format("YYYY-MM-DD"));
                        if (json_sector.hasOwnProperty(day)) {
                            mes_sector.push(json_sector[day].avg);
                        } else {
                            mes_sector.push(0);
                        }

                        if (json_nif.hasOwnProperty(day)) {
                            mes_nif.push(json_nif[day].avg);
                        } else {
                            mes_nif.push(0);
                        }
                    }
                    fs.appendFileSync('./files/estadisticas_mes_stats_sector.txt', `BuscarDatosGlobal;DescomprimirFacturas;TratarFacturas\n${buscar_datos_fin};${0};${tratar_facturas_fin - 0}\n`);
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

    return new Promise((resolve, reject) => {
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
                    var buscar_datos_start = performance.now();
                    Factura.aggregate([
                        {
                            $match: {
                                nif: nif,
                                fecha: {
                                    $gte: new Date("2021-01-01"),
                                    $lte: new Date("2021-03-28")
                                }
                            }
                        },
                        {
                            $project: {
                                fecha: 1,
                                cantidad: 1
                            }
                        }
                    ], (err, facturas) => {
                        var buscar_datos_fin = performance.now() - buscar_datos_start;
                        if (err) reject(err);
                        var json = {};
                        var tratar_facturas_start = performance.now();
                        facturas.forEach((factura, index) => {
                            let fecha = moment(factura.fecha).format("DD-MM");
                            if (!json.hasOwnProperty(fecha)) {
                                json[fecha] = {
                                    importe: 0,
                                    cantidad: 0,
                                    avg: 0
                                }
                            }

                            json[fecha].importe += factura.cantidad;
                            json[fecha].cantidad++;
                            json[fecha].avg = json[fecha].importe / json[fecha].cantidad;

                        });//End ForEach
                        var tratar_facturas_fin = performance.now() - tratar_facturas_start;
                        var triMes_nif = [];
                        for (var t = moment("2021-01-04").toDate(); t <= moment("2021-03-28").toDate(); t = moment(t).add(1, 'days').toDate()) {
                            let fecha = moment(t).format("DD-MM");
                            if (json.hasOwnProperty(fecha)) {
                                triMes_nif.push(json[fecha].avg);
                            } else {
                                triMes_nif.push(0);
                            }
                        }
                        fs.appendFileSync('./files/estadisticas_triMes_stats_nif.txt', `NIF;BuscarDatosGlobal;DescomprimirFacturas;TratarFacturas\n${nif};${buscar_datos_fin};${0};${tratar_facturas_fin - 0}\n`);
                        fs.appendFileSync('./estadisticas/2021-01-01_2021-03-28_global.txt', `triMes_${nif} // [${triMes_nif.toString()}]\n`);
                        resolve("OK");

                    });


                } else {
                    resolve("OK");
                }//end if
            } else {//No existe la estadistica global ni la del nif, asi que tengo que crearlas
                //console.log("No existe fichero");
                var nif_list = companies_nif_list.slice(0, 3000).map(c => c[0]);
                const date_start = new Date("2021-01-01T00:00:00");
                const date_fin = new Date("2021-03-28T00:00:00");
                var buscar_datos_start = performance.now();
                Factura.aggregate([
                    {
                        $match: {
                            nif: {
                                $in: nif_list
                            },
                            fecha: {
                                $gte: date_start,
                                $lte: date_fin
                            }
                        }
                    },
                    {
                        $project: {
                            nif: 1,
                            fecha: 1,
                            cantidad: 1
                        }
                    }
                ], (err, query_semana_result) => {
                    var buscar_datos_fin = performance.now() - buscar_datos_start;
                    console.log("Busqueda Fin");
                    var json_sector = {};
                    var json_nif = {};
                    var tratar_facturas_start = performance.now();

                    query_semana_result.forEach((factura, index) => {

                        let dia = moment(factura.fecha).format("DD-MM");
                        let importe = factura.cantidad;

                        if (!json_sector.hasOwnProperty(dia)) {
                            json_sector[dia] = {
                                importe: 0,
                                cantidad: 0,
                                avg: 0
                            }
                        }

                        json_sector[dia].importe += importe;
                        json_sector[dia].cantidad++;
                        json_sector[dia].avg = json_sector[dia].importe / json_sector[dia].cantidad;


                        if (factura.nif == nif) {
                            if (!json_nif.hasOwnProperty(dia)) {
                                json_nif[dia] = {
                                    importe: 0,
                                    cantidad: 0,
                                    avg: 0
                                }
                            }

                            json_nif[dia].importe += importe;
                            json_nif[dia].cantidad++;
                            json_nif[dia].avg = json_nif[dia].importe / json_nif[dia].cantidad;
                        }
                    });
                    //}//end for
                    var tratar_facturas_fin = performance.now() - tratar_facturas_start;
                    var mes_labels = [];
                    var mes_nif = [];
                    var mes_sector = [];

                    //console.log(json_sector);

                    for (var i = moment(date_start).toDate(); i <= moment(date_fin).toDate(); i = moment(i).add(1, 'days')) {

                        let day = moment(i).format("DD-MM");
                        mes_labels.push(moment(i).format("YYYY-MM-DD"));
                        if (json_sector.hasOwnProperty(day)) {
                            mes_sector.push(json_sector[day].avg);
                        } else {
                            mes_sector.push(0);
                        }

                        if (json_nif.hasOwnProperty(day)) {
                            mes_nif.push(json_nif[day].avg);
                        } else {
                            mes_nif.push(0);
                        }
                    }
                    fs.appendFileSync('./files/estadisticas_triMes_stats_sector.txt', `BuscarDatosGlobal;DescomprimirFacturas;TratarFacturas\n${buscar_datos_fin};${0};${tratar_facturas_fin - 0}\n`);
                    fs.appendFileSync('./estadisticas/2021-01-01_2021-03-28_global.txt', `triMes_labels // ["${mes_labels.join('","')}"]\ntriMes_sector // [${mes_sector.toString()}]\ntriMes_${nif} // [${mes_nif.toString()}]\n`);
                    resolve("OK");
                });

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
        //const res = await createEstadisticaDiaAgrupadas(nif);
        console.log("Dia --> " + res);
        try {
            const res_1 = await createEstadisticaSemanal(nif);
            //const res_1123 = createEstadisticaSemanalAgrupadas(nif);
            console.log("Semana --> " + res_1);
            try {
                const res_2 = await createEstadisticaMensual(nif);
                //const res_2 = await createEstadisticaSemanalAgrupadas(nif);
                console.log("Mes --> " + res_2);
                try {
                    //const res_3 = await createEstadisticaTrimestre(nif);
                    //const res_3 = await createEstadisticaSemanalAgrupadas(nif);
                    //console.log("TriMes --> " + res_3);
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
                    /*
                                        var triMes_file = fs.readFileSync('./estadisticas/2021-01-01_2021-03-28_global.txt').toString().split('\n');
                                        var triMes_labels = JSON.parse(triMes_file[0].split(" // ")[1]);
                                        var triMes_sector = JSON.parse(triMes_file[1].split(" // ")[1]);
                                        var triMes_nif = JSON.parse(triMes_file.filter(l_1 => l_1.split(" // ")[0] == `triMes_${nif}`)[0].split(" // ")[1]);
                    */


                    return {
                        dia_nif: dia_nif,
                        dia_sector: dia_sector,
                        dia_labels: dia_labels,
                        semana_nif: semana_nif,
                        semana_sector: semana_sector,
                        semana_labels: semana_labels,
                        mes_nif: mes_nif,
                        mes_sector: mes_sector,
                        mes_labels: mes_labels
                        //triMes_labels: triMes_labels,
                        //triMes_nif: triMes_nif,
                        //triMes_sector: triMes_sector
                    };
                } catch (err_3) {
                    //throw err_3;
                    return null;
                }
            } catch (err_2) {
                //throw err_2
                return null;
            }
        } catch (err) {
            //throw err;
            return null;
        }

    } catch (err_1) {
        //throw err_1;
        return null;
    }
}


module.exports = controller;