import { useCallback, useEffect, useState } from 'react';
import axios from 'axios';
import { FilePicker } from './components/FilePicker';

import styles from './App.module.css';
import { Button, Card, Checkbox, Flex } from '@gravity-ui/uikit';
import { useForm } from 'react-hook-form';

export const App: React.FC = () => {
	// const [groups, setGroups] = useState({});
	const [files, setFiles] = useState<File[]>([]);
	const { watch, setValue } = useForm<{
		columns: Array<{ content: string; value: boolean }>;
	}>({
		defaultValues: { columns: [] },
	});
	const [groups, setGroups] = useState({});

	const generateDatabase = useCallback(async () => {
		const formData = new FormData();
		for (const file of files) {
			formData.set('files', file);
		}
		await axios.post('http://localhost:8000/generate', formData);

		const {
			data: { headers },
		} = await axios.get<{ headers: string[] }>('http://localhost:8000/headers');

		setValue(
			'columns',
			headers.map(columnName => ({ content: columnName, value: false }))
		);

		const { data } = await axios.get('http://localhost:8000/groups');
		setGroups(data);
	}, [files, setValue]);

	useEffect(() => {
		console.log(watch('columns'));
	});

	return (
		<div className={styles.container}>
			<Card className={styles.card}>
				<FilePicker files={files} onChange={newFiles => setFiles(newFiles)} />
			</Card>

			<Flex direction="column" gap={1}>
				{watch('columns').map(({ content }, index) => (
					<Checkbox
						size="l"
						key={content}
						content={content}
						onUpdate={checked => setValue(`columns.${index}`, { content, value: checked })}
					/>
				))}
			</Flex>

			<Button
				size="xl"
				onClick={() => {
					generateDatabase();
				}}
			>
				Загрузить
			</Button>

			<pre>{JSON.stringify(groups, null, 2)}</pre>
		</div>
	);
};

export default App;
