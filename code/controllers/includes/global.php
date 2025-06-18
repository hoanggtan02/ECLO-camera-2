<?php
    if (!defined('ECLO')) die("Hacking attempt");
    echo $app->component('header');
    require_once $templatePath;
    echo $app->component('footer');
?>