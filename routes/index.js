var express = require('express');
var projectController = require('../controller/controller');
var router = express.Router();


//Middleware
var multipart = require('connect-multiparty');
var multipartMiddleware = multipart({uploadDir: './uploads'});


router.get('/', projectController.index);
router.get('/tctest', projectController.tctest);
router.post('/tctest', multipartMiddleware, projectController.fileCompress);

/* GET home page. *//*
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express', page:'index' });
});

router.get('/tctest', function(req, res, next) {
  res.render('index', { title: 'Express', page:'tctest' });
});*/




module.exports = router;
