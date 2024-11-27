import { Xmark } from '@gravity-ui/icons';
import { Button, FilePreview, Flex } from '@gravity-ui/uikit';
import { useRef } from 'react';

import styles from './FilePicker.module.css';

interface Props {
	onChange: (file: File[]) => void;
	files: File[];
	disabled?: boolean;
	error?: string;
}

export const FilePicker: React.FC<Props> = ({ onChange, files, disabled = false, error }) => {
	const inputFileRef = useRef<HTMLInputElement | null>(null);

	return (
		<Flex gap={1} direction={'column'}>
			<input
				ref={inputFileRef}
				type="file"
				accept=".csv"
				onChange={event => {
					onChange([...(event.target.files ?? [])]);
					inputFileRef.current!.value = '';
				}}
				hidden
				multiple
				disabled={disabled}
			/>
			{files.length > 0 ? (
				<div className={styles.grid}>
					{[...files].map((file, index) => (
						<FilePreview
							key={file.name}
							file={{ name: file.name, type: 'table' } as File}
							actions={[
								{
									icon: <Xmark width={14} height={14} />,
									title: 'Close',
									onClick: () => {
										const newFiles = [...files];
										newFiles.splice(index, 1);
										onChange(newFiles);
									},
								},
							]}
						/>
					))}
				</div>
			) : (
				'Файлы не выбраны'
			)}
			<Button
				onClick={() => inputFileRef.current?.click()}
				style={{ width: '150px', alignSelf: 'flex-end' }}
				disabled={disabled}
			>
				{files.length > 0 ? 'Изменить файлы' : 'Загрузить файлы'}
			</Button>
			{error && <div className="g-outer-additional-content__error">{error}</div>}
		</Flex>
	);
};
