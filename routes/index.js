var express = require('express');
var projectController = require('../controller/controller');
var router = express.Router();


//Middleware
var multipart = require('connect-multiparty');
var multipartMiddleware = multipart({uploadDir: './uploads'});


router.get('/', projectController.index);

/**PRUEBAS TECNICAS DE COMPRESION */
router.get('/tctest', projectController.tctest);

/**INSERCION DE FACTURAS */
router.get('/insercion', projectController.insertFactura);
router.post('/insercion', multipartMiddleware, projectController.insercionFacturas);
router.get('/insertmany', projectController.insertManyView);
router.get('/insertmanyagrupadas', projectController.insertMany);

/**OBTENCION DE FACTURAS */
router.get('/getfactura', projectController.getFactura);
router.get('/gr', projectController.gr);

/**Estadisticas por sectores */
router.get('/statistics', projectController.statistics);
router.get('/showstatistics', projectController.showStatistics);
router.get('/estadisticaspormenor', projectController.showStatisticsMenor);
router.get('/estadisticaspormayor', projectController.showStatisticsMayor);

router.get("/insertfacturasestadisticas", projectController.insertFacturasEstadisticas);
router.get("/createdata", projectController.createData);
router.get("/agruparfacturas", projectController.agruparFacturas);



module.exports = router;
