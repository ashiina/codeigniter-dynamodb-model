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
	 * @param string tableName
	 * @param array conditions
	 * return array items
	 *
	 * example $conditions format:
array(
        'Id ='     => (int)251,
        'Date ~'   => array ( (float)1376030000, (float)1376040000)
));
	 */
	public function query ($tableName, $conditions) {
		try {
			$qResult = $this->ddbClient->query(array(
				'TableName'		=> $tableName,
				'HashKeyValue'	=> array(
					$this->_getEnumType($primaryKey) => $primaryKey,
				),
				'KeyConditions'	=> $this->_formatQueryCondition($conditions)
			));
		} catch (Exception $e) {
			var_dump($e->getMessage());
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
));
	 */
	public function putItem ($tableName, $item) {
		try {
			$formattedItem = $this->_formatPutItem($item);
			var_dump($formattedItem);
			$this->ddbClient->putItem(array(
				'TableName'		=> $tableName,
				'Item'			=> $formattedItem
			));
		} catch (Exception $e) {
			var_dump($e->getMessage());
		}
	}

	/*
	 * (protected) format item into dynamodb style.
	 * example result format:
    'Item'      => array(
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
