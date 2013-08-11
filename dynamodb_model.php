<?
/*
 dynamodb class
 Wrapper for aws-sdk-2 dynamodb
*/ 
class Dynamodb_model extends CI_Model {
	/*
	 * dynamodb client
	 */
	protected $ddbClient;

	/*
	 * prefix for enum classes,
	 * since cannot directly access just by saying 'Type'.
	 */
	protected $enumTypeClass = 'Aws\DynamoDb\Enum\Type';
	protected $enumCOClass = 'Aws\DynamoDb\Enum\ComparisonOperator';

	/*
	 * batch execution limit
	 */
	protected $BATCH_LIMIT = 25;

	/*
	 * Constructor
	 */
	public function __construct() {
		$this->load->library('awslib');
		$this->ddbClient = $this->awslib->aws->get('dynamodb');
	}

	/*
	 * query wrapper for dynamodb.
	 * It will take a simple assoc-array, and turn it into a
	 * DynamoDB API compliant format, then send a request.
	 * The conditions' keys will take a operator of the following:
	 * < , <= , > , >= , = , != , ~(BETWEEN operator)
	 *
	 * Optionally pass any other query API options in $options.
	 * 
	 * @param string tableName
	 * @param array  conditions
	 * @param int    limit
	 * @param array  options
	 * return array  items
	 *
	 * example $conditions format:
array(
        'Id ='     => (int)251,
        'Date ~'   => array ( (float)1376030000, (float)1376040000)
));
	 */
	public function query ($tableName, $conditions, $limit=10, $options=array()) {
		try {
			$qRequest = array(
				'TableName'		=> $tableName,
				'HashKeyValue'	=> array(
					$this->_getEnumType($primaryKey) => $primaryKey,
				),
				'KeyConditions'	=> $this->_formatQueryCondition($conditions)
			);
			$qRequest['Limit'] = $limit;
			$qRequest = array_merge($qRequest, $options);
			$qResult = $this->ddbClient->query($qRequest);
		} catch (Exception $e) {
			error_log($e->getMessage());
		}
		return $this->_formatResult($qResult);
	}

	/*
	 * putItem wrapper for dynamodb.
	 * It will take a simple assoc-array, and turn it into a
	 * DynamoDB API compliant format, then send a request.
	 * Make sure to cast each value, since the program will dynamically
	 * set the appropriate Enum based on the variable type
	 *
	 * @param string tableName
	 * @param array item
	 * return void
	 *
	 * example $item format:
array(
        'Id'       => (int)251,
        'Comment'  => (string)"hello",
        'Date'     => (float)1376038837
);
	 */
	public function putItem ($tableName, $item) {
		try {
			$formattedItem = $this->_formatPutItem($item);
			$this->ddbClient->putItem(array(
				'TableName'		=> $tableName,
				'Item'			=> $formattedItem
			));
			return true;
		} catch (Exception $e) {
			error_log($e->getMessage());
			return false;
		}
	}

	/*
	 * batchWriteItem wrapper for dynamodb, only for putItem requests.
	 * It will take an array of assoc-arrays for items to insert, 
	 * and turn it into a DynamoDB API compliant format, then send a request.
	 * Make sure to cast each value, since the program will dynamically
	 * set the appropriate Enum based on the variable type.
	 * 
	 * The DynamoDB API only takes max 25items per batch, 
	 * so larger batches will be automatically divided into multiple requests.
	 *
	 * @param string tableName
	 * @param array items
	 * return void
	 *
	 * example $items format:
array(
    array(
        'Id'       => (int)251,
        'Comment'  => (string)"hello",
        'Date'     => (float)1376038837
    ),
    array(
        'Id'       => (int)251,
        'Comment'  => (string)"goodbye",
        'Date'     => (float)1376038880
    )
);
	 */
	public function batchPutItem ($tableName, $items) {
		$formattedItems = array();
		$i=1;
		foreach ($items as $item) {
			$formattedItems[] = array(
				"PutRequest" => array(
					"Item" => $this->_formatPutItem($item)
				)
			);

			// execute on each batch limit
			if (count($formattedItems) >= $this->BATCH_LIMIT) {
				error_log("execute batch write: $i");
				$this->_executeBatchWriteItem($tableName, $formattedItems);
				unset($formattedItems);
				$formattedItems = array();
			}
			$i++;
		}
		$this->_executeBatchWriteItem($tableName, $formattedItems);
		unset($items);
		unset($formattedItems);
	}

	protected function _executeBatchWriteItem ($tableName, $formattedItems) {
		try {
			$this->ddbClient->batchWriteItem(array(
				'RequestItems'	=> array(
					$tableName => $formattedItems
				)
			));
		} catch (Exception $e) {
			error_log($e->getMessage());
		}
	}

	/*
	 * (protected) format item into dynamodb style.
	 * example result format:
    array(
        'id'    => array('N' => '3000'),
        'time'  => array('N' => $time),
        'error' => array('S' => 'Out of bounds'),
        'data'  => array('B' => $data)
    )
	 */
	protected function _formatPutItem ($item) {
		$formattedItem = array();
		foreach ($item as $attrKey => $attrVal) {
			$type = $this->_getEnumType($attrVal);
			if (!$type) continue;

			$formattedItem[$attrKey] = array(
				$type	=> (string)$attrVal
			);
		}
		return $formattedItem;
	}

	protected function _formatQueryCondition ($query) {
		$formattedCondition = array();
		foreach ($query as $qKey => $qVal) {
			list ($key,$symbol) = explode(' ', $qKey);

			$co = '';
			$attrValues = array();
			$co = $this->_getEnumCO($symbol);

			if (is_array($qVal)) {
				foreach ($qVal as $val) {
					$attrValues[] = 
						array( $this->_getEnumType($val) => (string)$val );
				}
			} else {
				$attrValues = array(
						array( $this->_getEnumType($qVal) => (string)$qVal )
				);
			}

			$formattedCondition[$key] = array(
				'ComparisonOperator'	=> $co,
				'AttributeValueList'	=> $attrValues
			);
		}
		return $formattedCondition;
	}

	protected function _formatResult ($response) {
		$formattedResult;
		foreach ($response['Items'] as $item) {
			$formattedItem = array();
			foreach ($item as $k => $v) {
				$itemValArr = array_values($v);
				$formattedItem[$k] = $itemValArr[0];
			}
			$formattedResult[] = $formattedItem;
		}
		return $formattedResult;
	}

	protected function _getEnumType ($val) {
		$_enumTypeClass = $this->enumTypeClass;
		$type;
		if (is_int($val)) 		$type = ${_enumTypeClass}::NUMBER;
		if (is_float($val)) 	$type = ${_enumTypeClass}::NUMBER;
		if (is_string($val)) 	$type = ${_enumTypeClass}::STRING;
		return $type;
	}

	protected function _getEnumCO ($symbol) {
		$_enumCOClass = $this->enumCOClass;
		$co;
		switch ($symbol) {
			case "<":
				$co = ${_enumCOClass}::LT;
				break;
			case "<=":
				$co = ${_enumCOClass}::LE;
				break;
			case ">":
				$co = ${_enumCOClass}::GT;
				break;
			case ">=":
				$co = ${_enumCOClass}::GE;
				break;
			case "=":
				$co = ${_enumCOClass}::EQ;
				break;
			case "!=":
				$co = ${_enumCOClass}::NE;
				break;
			case "~":
				$co = ${_enumCOClass}::BETWEEN;
				break;
			default:
				$co = ${_enumCOClass}::EQ;
				break;
		}
		return $co;
	}
}

?>
