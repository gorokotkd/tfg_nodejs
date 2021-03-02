var express = require('express');
var projectController = require('../controller/controller');
var router = express.Router();




router.get('/', projectController.index);
router.get('/tctest', projectController.tctest);
router.post('/tctest', projectController.fileCompress);

/* GET home page. *//*
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express', page:'index' });
});

router.get('/tctest', function(req, res, next) {
  res.render('index', { title: 'Express', page:'tctest' });
});*/




module.exports = router;
