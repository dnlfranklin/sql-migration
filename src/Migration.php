<?php

namespace SqlMigration;

use SqlMigration\Catalog\Schema;

abstract class Migration{
    
    abstract public function up():Schema;

}
